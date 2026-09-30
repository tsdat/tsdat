import os
import shutil
import subprocess
from pathlib import Path
import sys

PROJECT_DIR = Path.cwd()  # cwd changes to the newly-generated project when this runs


def main():
    print("Template created for {{ cookiecutter.vap_name }}. Checking selections...")

    if "{{ cookiecutter.use_custom_data_converter }}" == "no":  # type: ignore
        os.remove(PROJECT_DIR / "converters.py")

    if "{{ cookiecutter.use_custom_qc }}" == "no":  # type: ignore
        os.remove(PROJECT_DIR / "qc.py")
        os.remove(PROJECT_DIR / "config" / "quality.yaml")

    if black := shutil.which("black"):
        subprocess.run([black, str(PROJECT_DIR)], check=True)

    print(f"Generated {{ cookiecutter.vap_name }} in {PROJECT_DIR}")


if __name__ == "__main__":
    main()
    sys.exit(0)
