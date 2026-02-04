# License & Derivative Notice for `mineru_core`

## ⚠️ Important Notice

The code under the **`mineru_core/`** directory contains logic **copied from and adapted based on the open-source project [MinerU](https://github.com/opendatalab/MinerU)**.

**MinerU is licensed under the GNU Affero General Public License v3.0 (AGPL-3.0)**.  
As a result, the code in this directory is **also subject to the AGPL-3.0 license**.

---

## 📜 License Statement

- **Upstream project**: MinerU  
- **Upstream license**: AGPL-3.0  
- **Scope of influence**:  
  All source files under `mineru_core/` that reuse, adapt, or reorganize logic from MinerU.

In accordance with the requirements of the AGPL-3.0 license:

- The original license terms and copyright notices are preserved.
- Any modifications or reorganizations of the copied logic remain licensed under **AGPL-3.0**.
- Users who receive or interact with this code (including via network services) are entitled to access the corresponding source code.

---

## 🧩 Relationship to the Rest of the Repository

This repository contains **multiple components with different roles**:

- `mineru_core/`  
  - Contains **AGPL-3.0 licensed derivative code** based on MinerU.
  - Governed by the AGPL-3.0 license.

- Other directories (e.g. Ray orchestration, pipeline glue code)  
  - May include original code written for this project.
  - **However**, due to linking and integration with `mineru_core/`, the **overall project is distributed under AGPL-3.0** to remain license-compliant.

---

## 🙏 Acknowledgement

We sincerely thank the **MinerU authors and contributors** for their excellent open-source work.  
This project would not be possible without the foundation provided by MinerU.

Original project:
- https://github.com/opendatalab/MinerU

---

## 📌 Note to Contributors

When contributing to code under `mineru_core/`:

- Assume **AGPL-3.0** applies by default.
- Do not remove or alter existing license headers.
- New files that are derived from or tightly coupled with MinerU logic should also carry AGPL-3.0 license headers.

If you intend to add **completely independent implementations**, please clearly document their origin and licensing.

---

## 📬 Questions

If you believe any licensing information here is incorrect or incomplete,  
please open an Issue so it can be reviewed and corrected promptly.
