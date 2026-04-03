import json, copy, tempfile
import yaml, requests
import subprocess
import os
from typing import Dict, List, Union, Tuple, Set, Optional
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.version import parse as parse_version
from packaging.version import Version
from pathlib import Path
        
class Environment:
    
    def __init__(self, env_input: dict | str = {"conda": [], "pip": []}):
        
        if isinstance(env_input, str):
            if env_input.endswith('.txt'):
                env_input = {"conda": [], "pip": [env_input]}
            elif '.' not in env_input or env_input.endswith('.yaml') or env_input.endswith('.yml'):
                env_input = {"conda": [env_input], "pip": []}
            else:
                raise ValueError(f"Invalid environment input string: {env_input}, please provide a valid conda environment name, a yaml/yml file, or a requirements.txt file.")

        self.input_env_dict = env_input
        
        self.conda_dependencies: Set[str] = set()
        self.pip_dependencies: Dict[str, Requirement] = {}
        self._extract_requirements()
        self.ray_style_env_name: Optional[dict] = None
    
    def get_conda_dependencies(self) -> Set[str]:

        return self.conda_dependencies
    
    def get_pip_dependencies(self) -> Dict[str, Requirement]:

        return self.pip_dependencies
    
    def check_inside(self, other_env_instance) -> bool:
        
        def check(target_reqs: Dict[str, Requirement], base_reqs: Dict[str, Requirement]) -> bool:

            for name, base_req in base_reqs.items():
                if name not in target_reqs or target_reqs[name].specifier != base_req.specifier:
                    return False

            return True
        
        for item in other_env_instance.get_conda_dependencies():
            if item not in self.conda_dependencies:
                return False
        
        return check(self.pip_dependencies, other_env_instance.get_pip_dependencies())

    def get_ray_style_runtime_env(self) -> str:
        if self.ray_style_env_name is None:
            self.ray_style_env_name = self._generate_ray_env_name()
        
        ray_env_name = self._serialize_env_name(self.ray_style_env_name)
        
        return json.loads(ray_env_name)
    
    def _generate_ray_env_name(self) -> dict:
        
        use_local_env = False
        
        if len(self.input_env_dict.get("conda", [])) == 1 and len(self.input_env_dict.get("pip", [])) == 0:
            name = self.input_env_dict.get("conda", [None])[0]
            use_local_env = isinstance(name, str) and not Path(name).suffix
        
        use_default_env = len(self.input_env_dict.get("conda", [])) == 0
        
        if use_local_env:
            return {"conda": self.input_env_dict.get("conda")[0]}
        
        self.resolve_pip_dependencies_with_uv()
        pip_list = []
        for _, req in self.pip_dependencies.items():
            pip_list.append(str(req))
        
        if use_default_env:
            return {"pip": pip_list}

        env_name = {"conda" : {"dependencies": []}}
        for req in self.conda_dependencies:
            env_name["conda"]["dependencies"].append(req)
        env_name["conda"]["dependencies"].append({"pip": pip_list})
            
        return env_name
        
    def _serialize_env_name(self, env_dict: dict) -> str:
        """将字典转为可作为 Key 的标准 JSON 字符串"""
        return json.dumps(env_dict, sort_keys=True)
    
    def _check_requirement_include(self, target: Requirement, base: Requirement) -> bool:
        
        flag = (
            target.name == base.name
            and target.extras == base.extras
            and target.url == base.url
            and target.marker == base.marker
            and target.install_mode == base.install_mode
        )
            
        flag = flag and (base.specifier == target.specifier or str(target.specifier) == "")
        return flag
    
    def _add_pip_req(self, req_str: str):
        """解析 pip 格式的依赖项"""
        
        req = Requirement(req_str)
        if req.name in self.pip_dependencies:
            self.pip_dependencies[req.name].specifier = self.pip_dependencies[req.name].specifier & req.specifier
        else:
            self.pip_dependencies[req.name] = req
            
    def _add_conda_req(self, req_str: str):
        """解析 conda 格式的依赖项"""
        
        if req_str not in self.conda_dependencies:
            self.conda_dependencies.add(req_str)

    def _parse_conda_dependency(self, dep: Union[str, dict]) -> List[Requirement]:
        """解析 conda 格式的依赖项"""
        reqs = []
        if isinstance(dep, str):
            self._add_conda_req(dep)
        elif isinstance(dep, dict) and "pip" in dep:
            for pip_dep in dep["pip"]:
                self._add_pip_req(pip_dep)

    def _resolve_local_conda_env(self, env_name: str) -> List[Requirement]:
        """调用 conda 命令行导出本地环境并解析"""
        try:
            # 使用 conda env export 获取完整依赖
            output = subprocess.check_output(
                ["conda", "env", "export", "-n", env_name, "--no-builds"], 
                encoding='utf-8'
            )
            data = yaml.safe_load(output)
            for dep in data.get('dependencies', []):
                self._parse_conda_dependency(dep)

        except Exception as e:
            print(f"Warning: Failed to resolve local conda env '{env_name}': {e}")
            return []

    def _parse_file(self, file_path: str):
        """解析 requirements.txt 或 yaml/yml 文件"""

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
            
        if file_path.endswith('.yaml') or file_path.endswith('.yml'):
            with open(file_path, 'r') as f:
                data = yaml.safe_load(f)
                for dep in data.get('dependencies', []):
                    self._parse_conda_dependency(dep)
        else: # 默认为 requirements.txt 格式
            with open(file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        self._add_pip_req(line)

    def _extract_requirements(self):
        """
        将输入格式转化为 {包名: 版本限制} 的字典
        """

        for item in self.input_env_dict.get("conda", []):
            if isinstance(item, dict):
                for dep in item.get("dependencies", []):
                    self._parse_conda_dependency(dep)
            elif isinstance(item, str):
                if item.endswith('.yaml') or item.endswith('.yml'):
                    self._parse_file(item)
                else:
                    self._resolve_local_conda_env(item)
        
        for item in self.input_env_dict.get("pip", []):
            
            if item.endswith('.txt'):
                self._parse_file(item)
            else:
                self._add_pip_req(item)
                
    def resolve_pip_dependencies_with_uv(self) -> None:
        """
        使用 uv 解析 self.pip_dependencies 并更新该字典为全量精确依赖
        """

        if not self.pip_dependencies:
            return

        with tempfile.TemporaryDirectory() as temp_dir:
            req_in_path = os.path.join(temp_dir, "requirements.in")
            req_out_path = os.path.join(temp_dir, "requirements.txt")

            # 1. 将现有的 Requirement 对象写入 requirements.in
            with open(req_in_path, "w", encoding="utf-8") as f:
                for req in self.pip_dependencies.values():
                    f.write(f"{str(req)}\n")

            # 2. 调用 uv pip compile 进行依赖解析
            try:
                subprocess.run(
                    ["uv", "pip", "compile", req_in_path, "-o", req_out_path],
                    check=True,
                    capture_output=True,
                    text=True
                )
            except subprocess.CalledProcessError as e:
                raise RuntimeError(f"uv 解析依赖失败:\n{e.stderr}")

            # 3. 解析生成的 requirements.txt
            resolved_dependencies: Dict[str, Requirement] = {}
            
            with open(req_out_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    
                    # 忽略空行、注释行（uv 默认会在头部写注释）
                    if not line or line.startswith("#") or line.startswith("-"):
                        continue
                    
                    # uv 编译后的文件中，依赖名后面可能会跟着 "# via ..." 注释， 我们只需要提取包名和版本规范部分
                    clean_line = line.split("#")[0].strip()
                    clean_line = clean_line.split("\\")[0].strip()
                    
                    if clean_line:
                        # 重新构建 Requirement 对象
                        req = Requirement(clean_line)
                        resolved_dependencies[req.name] = req

            # 4. 替换原来的字典
            self.pip_dependencies = resolved_dependencies


class EnvironmentRegistry():

    def __init__(self, name):

        self._name = name
        self._env_map_ray_style: Dict[str, Dict] = {}
        self._env_map_class: Dict[str, Environment] = {}
        
    def _do_register(self, env_instance: Environment, name: str):
        
        for _name, _env in self._env_map_class.items():
            if _env.check_inside(env_instance):
                self._env_map_class[name] = _env
                self._env_map_ray_style[name] = self._env_map_ray_style[_name]
                
                return

        self._env_map_class[name] = env_instance
        self._env_map_ray_style[name] = env_instance.get_ray_style_runtime_env()


    def register(self, *, _obj = None,  name: Optional[str] = None, env_input : Optional[Dict | str] = None):
        """
        注册环境，可以类的装饰器使用，也可以通过输入环境定义字典来注册，也可以直接注册一个类的实力。注册时会自动解析环境依赖并生成 Ray 运行时环境格式的定义。
        
        - env_input 的格式：
        {
            "conda" : [], list中包含若干已有环境名、yaml文件或字典，表示需求的环境是list中环境的合集。
            "pip": [], list中包含若干包或requirements.txt文件，表示需要安装的pip包。
        } 或者直接输入一个字符串表示本地已有的环境名称或文件名。
        
        @EnvironmentRegistry.register(name='env6')
        class TestEnv(Environment):
            # 此时注册表会自动生成一个TestEnv实例，并将其注册到注册表中，注册名为 'env6'（如果不提供 name，则默认为类名）。TestEnv 类需要继承自 Environment，并在 __init__ 方法中定义环境输入字典。
        
        @EnvironmentRegistry.register()
        class TestEnv(Environment):
        
        EnvironmentRegistry.register(name="env1", env_input={
            "conda": [
                {"dependencies": ["python=3.8", "numpy=1.21.0", "pandas=1.3.0", {"pip": ["scipy==1.7.0"]}]}
            ],
            "pip": [
                "requests==2.26.0"
            ]
        })
        
        test_env = TestEnv()
        EnvironmentRegistry.register(_obj=test_env, name="env2")
        """

        if _obj is not None:
            
            if isinstance(_obj, Environment):
                
                self._do_register(_obj, name if name is not None else _obj.__class__.__name__)
                return
            
            if not issubclass(_obj.__class__, Environment):
                raise TypeError("Only Environment subclasses can be registered by decorator style.")
            
            self._do_register(_obj(), _obj.__name__ if name is None else name)
            return
            
        elif env_input is None:
            
            def deco(class_obj):
                env_instance = class_obj()
                self._do_register(env_instance, class_obj.__name__ if name is None else name)
                
                return class_obj
    
            return deco
        
        if name is None:
            raise ValueError("Environment name must be provided when registering with env_input_dict.")

        self._do_register(Environment(env_input), name)
        
    def get_ray_style_env(self, name: str) -> str:

        if name not in self._env_map_ray_style:
            raise KeyError(f"Environment '{name}' is not registered in the registry '{self._name}'.")

        return self._env_map_ray_style.get(name)


EnvRegistry = EnvironmentRegistry("GlobalEnvRegistry")


if __name__ == "__main__":
    
    env_dict_1 = {
        "conda": [
            {"dependencies": ["python=3.8", "numpy=1.21.0", "pandas=1.3.0", {"pip": ["scipy==1.7.0"]}]}
        ],
        "pip": [
            "requests==2.26.0"
        ]
    }
    
    env_dict_2 = {
        "conda": [
            {"dependencies": ["python=3.8", {"pip": ["scipy==1.7.0"]}]}
        ],
        "pip": [
            "requests==2.26.0"
        ]
    }
    
    env_dict_3 = "crnet.yaml"
    
    env_dict_4 = "mineru"
    
    env_dict_5 = {
        "pip": ["mineru-vl-utils>=0.1.0", "mineru-vl-utils<=0.1.1"]
    }
    
    EnvRegistry.register(name="env1", env_input=env_dict_1)
    EnvRegistry.register(name="env2", env_input=env_dict_2)
    EnvRegistry.register(name="env3", env_input=env_dict_3)
    EnvRegistry.register(name="env4", env_input=env_dict_4)
    EnvRegistry.register(name="env5", env_input=env_dict_5)
    
    print(EnvRegistry.get_ray_style_env("env1"))
    print(EnvRegistry.get_ray_style_env("env2"))
    print(EnvRegistry.get_ray_style_env("env3"))
    print(EnvRegistry.get_ray_style_env("env4"))
    print(EnvRegistry.get_ray_style_env("env5"))
    
    @EnvRegistry.register(name='env6')
    class TestEnv(Environment):
        
        def __init__(self):
            super().__init__({"conda": ["mineru"], "pip": []})
            
    test_env = Environment({"conda": ["test"], "pip": []})
    EnvRegistry.register(_obj=test_env, name="env7")
                
    print(EnvRegistry.get_ray_style_env("env6"))
    print(EnvRegistry.get_ray_style_env("env7"))