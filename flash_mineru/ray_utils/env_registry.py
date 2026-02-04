import json, copy
import yaml, requests
import subprocess
import os
from typing import Dict, List, Union, Tuple, Set, Optional
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.version import parse as parse_version
from packaging.version import Version

class NewRequirement(Requirement):

    def __init__(self, requirement_string: str, install_mode: str = 'conda'):
        super().__init__(requirement_string)
        self.install_mode = install_mode
        
class Environment:
    
    # TODO: 支持更多环境信息，例如环境变量、docker等，尽量与ray的runtime env对齐。目前仅支持conda和pip 的 环境管理、一对多、合并等。
    
    def __init__(self, input_env_dict: dict = None):
        
        if input_env_dict is not None:
            self.input_env_dict = input_env_dict
        self.run_with_default_env = input_env_dict.get("conda", []) == []

    # TODO: 重新实现环境依赖解析和管理。
    #     if self.run_with_default_env:
    #         input_env_dict["conda"] = os.environ.get("CONDA_DEFAULT_ENV")
            
    #     self.requires = self._extract_requirements(input_env_dict)
    #     self.ray_style_env_name = self._generate_ray_env_name()
        
    # def get_ray_runtime_env(self) -> dict:
    #     if self.ray_style_env_name is None:
    #         self.ray_style_env_name = self._generate_ray_env_name()
            
    #     return self.ray_style_env_name
        
    # def get_requires(self) -> Dict[str, NewRequirement]:
    #     return self.requires
    
    # def _generate_ray_env_name(self) -> dict:
        
    #     conda_info = self.input_env_dict.get("conda", [])
    #     if len(conda_info) == 1 and isinstance(conda_info[0], str) and len(self.input_env_dict.get("pip", [])) == 0:
    #         # 仅指定了单个已有 conda 环境名称，直接使用
    #         return {"conda": conda_info[0]}

    #     env_name = {}
    #     if self.run_with_default_env:
    #         env_name["pip"] = []
    #         for req in self.requires.values():
    #             env_name["pip"].append(str(req))
    #     else:
    #         pip_list = []
    #         env_name["conda"]={"dependencies":[]}
            
    #         for req in self.requires.values():
    #             if req.install_mode == 'conda':
    #                 env_name["conda"]["dependencies"].append(str(req))
    #             else:
    #                 pip_list.append(str(req))
        
    #         if len(pip_list) > 0:
    #             env_name["conda"]["dependencies"].append({"pip": pip_list})
            
    #     return env_name
        
    # def _serialize_key(self, env_dict: dict) -> str:
    #     """将字典转为可作为 Key 的标准 JSON 字符串"""
    #     return json.dumps(env_dict, sort_keys=True)
    
    # def _check_requirement_include(self, target: NewRequirement, base: NewRequirement) -> bool:
        
    #     flag = (
    #         target.name == base.name
    #         and target.extras == base.extras
    #         and target.url == base.url
    #         and target.marker == base.marker
    #         and target.install_mode == base.install_mode
    #     )
            
    #     flag = flag and (base.specifier == target.specifier or str(target.specifier) == "")
    #     return flag
    #     # TODO ：实现检测包版本之间的包含关系，目前仅能检测完全相等的情况或未指定版本的情况
    #     # versions = set()
        
    #     # if target.install_mode == 'pip':
    #     #     url = f"https://pypi.org/pypi/{pkg_name}/json"
    #     #     resp = requests.get(url, timeout=10)
    #     #     resp.raise_for_status()

    #     #     data = resp.json()
    #     #     versions = (Version(v) for v in data["releases"].keys())
    #     # else:
    #     #     cmd = ["conda", "search", pkg_name]
    #     #     result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    #     #     versions = set()
    #     #     for line in result.stdout.splitlines():
    #     #         line = line.strip()
    #     #         if line.startswith(pkg_name):
    #     #             parts = line.split()
    #     #             if len(parts) >= 2:
    #     #                 versions.add(parts[1])
        
    #     # for ver in versions:
    #     #     if ver in base.specifier and not ver in target.specifier:
    #     #         return False

    #     # return True
        

    # def _parse_conda_dependency(self, dep: Union[str, dict]) -> List[NewRequirement]:
    #     """解析 conda 格式的依赖项"""
    #     reqs = []
    #     if isinstance(dep, str):
    #         # 处理简单的包名，如 "numpy=1.26.4" 或 "python>3.8"
    #         # Conda 的单个 '=' 需要转换为 '==' 以适应 packaging 库，除非它是 build string
    #         if '=' in dep and '==' not in dep and '<' not in dep and '>' not in dep:
    #              parts = dep.split('=')
    #              if len(parts) == 2:
    #                  dep = f"{parts[0]}=={parts[1]}"
    #         try:
    #             reqs.append(NewRequirement(dep, 'conda'))
    #         except:
    #             pass # 忽略无法解析的非标准字符串
    #     elif isinstance(dep, dict) and "pip" in dep:
    #         for pip_dep in dep["pip"]:
    #             reqs.append(NewRequirement(pip_dep, 'pip'))
    #     return reqs

    # def _resolve_local_conda_env(self, env_name: str) -> List[NewRequirement]:
    #     """调用 conda 命令行导出本地环境并解析"""
    #     try:
    #         # 使用 conda env export 获取完整依赖
    #         output = subprocess.check_output(
    #             ["conda", "env", "export", "-n", env_name, "--no-builds"], 
    #             encoding='utf-8'
    #         )
    #         data = yaml.safe_load(output)
    #         reqs = []
    #         for dep in data.get('dependencies', []):
    #             reqs.extend(self._parse_conda_dependency(dep))
    #         return reqs
    #     except Exception as e:
    #         print(f"Warning: Failed to resolve local conda env '{env_name}': {e}")
    #         return []

    # def _parse_file(self, file_path: str) -> List[NewRequirement]:
    #     """解析 requirements.txt 或 yaml/yml 文件"""
    #     reqs = []
    #     if not os.path.exists(file_path):
    #         raise FileNotFoundError(f"File not found: {file_path}")
            
    #     if file_path.endswith('.yaml') or file_path.endswith('.yml'):
    #         with open(file_path, 'r') as f:
    #             data = yaml.safe_load(f)
    #             for dep in data.get('dependencies', []):
    #                 reqs.extend(self._parse_conda_dependency(dep))
    #     else: # 默认为 requirements.txt 格式
    #         with open(file_path, 'r') as f:
    #             for line in f:
    #                 line = line.strip()
    #                 if line and not line.startswith('#'):
    #                     reqs.append(NewRequirement(line, 'pip'))
    #     return reqs

    # def _extract_requirements(self, input_config: dict) -> Dict[str, NewRequirement]:
    #     """
    #     将输入格式转化为 {包名: 版本限制} 的字典
    #     """
    #     all_reqs: List[NewRequirement] = []

    #     for item in input_config.get("conda", []):
    #         if isinstance(item, dict):
    #             for dep in item.get("dependencies", []):
    #                 all_reqs.extend(self._parse_conda_dependency(dep))
    #         elif isinstance(item, str):
    #             if item.endswith('.yaml') or item.endswith('.yml'):
    #                 all_reqs.extend(self._parse_file(item))
    #             else:
    #                 all_reqs.extend(self._resolve_local_conda_env(item))
        
    #     pip_pkgs = []
    #     for item in input_config.get("pip", []):
            
    #         if item.endswith('.txt'):
    #             parsed_list = self._parse_file(item)
    #             all_reqs.extend(parsed_list)
    #             pip_pkgs.extend(parsed_list)
    #         else:
    #             all_reqs.append(NewRequirement(item, 'pip'))
    #             pip_pkgs.append(NewRequirement(item, 'pip'))

    #     merged_specs: Dict[str, NewRequirement] = {}
    #     for req in all_reqs:
    #         name = req.name.lower()
    #         if name not in merged_specs:
    #             merged_specs[name] = req
    #         else:
    #             if req.install_mode != merged_specs[name].install_mode:
    #                 merged_specs[name].install_mode = 'pip'
    #             if req.specifier != merged_specs[name].specifier:
    #                 merged_specs[name].specifier = merged_specs[name].specifier & req.specifier

    #     return merged_specs
    
    # def check_inside(self, base_env):
    #     """
    #     判断当前环境是否包含在 base 环境内
    #     """
    #     base_requires = base_env.get_requires()
    #     for name, target_require in self.requires.items():

    #         if name not in base_requires:
    #             return False

    #         base_require = base_requires[name]

    #         if not self._check_requirement_include(target_require, base_require):
    #             return False

    #     return True

    # # TODO: 在已有环境基础上仅额外安装少数缺失包，目前看好像ray没有留这样的接口，只能全新建环境。
    # # def _calculate_missing(self, base_requires: Dict[str, NewRequirement]) -> Set[NewRequirement]:
    # #     """
    # #     计算 target 相对于 base 需要“额外安装”或“由于冲突需要重装”的包。
    # #     返回需要 pip 安装的包定义字符串列表。
    # #     """
    # #     install_list = set()
        
    # #     for name, target_require in self.requires.items():
    # #         # Base 中完全没有这个包 -> 需要安装
    # #         if name not in base_requires:
    # #             install_list.add(target_require)
    # #             continue
            
    # #         # Base 中有这个包，但是版本区间不兼容 (Conflict) -> 视为需要重新安装覆盖
    # #         base_require = base_requires[name]

    # #         if not self._check_requirement_include(target_require, base_require):
    # #              install_list.add(target_require)

    # #     return install_list

class EnvironmentRegistry():

    def __init__(self, name):

        self._name = name
        self._env_map_requirements = {}
        self._env_map_ray_style = {}
        self._env_map_class = {}
        
    def _do_register(self, env_instance: Environment, name: str, env_input_dict : Optional[Dict] = None):
        
        self._env_map_class[name] = env_instance
        self._env_map_requirements[name] = env_instance.get_requires()
        self._env_map_ray_style[name] = env_instance.get_ray_runtime_env()


    def register(self, name: Optional[str] = None, env_input_dict : Optional[Dict] = None):
        
        if name is None:
            def deco(class_obj):
                env_instance = class_obj()
                self._do_register(env_instance, class_obj.__name__, env_instance.input_env_dict)
                
                return class_obj
    
            return deco
        
        self._do_register(Environment(env_input_dict), name, env_input_dict)
        
    def get_ray_style_env(self, name: str) -> dict:

        return self._env_map_ray_style.get(name)



EnvRegistry = EnvironmentRegistry("GlobalEnvRegistry")