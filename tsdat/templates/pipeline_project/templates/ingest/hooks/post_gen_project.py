import os
import shutil
import subprocess
from pathlib import Path
import sys

PROJECT_DIR = Path.cwd()  # cwd changes to the newly-generated project when this runs


def main():
    print("Template created for {{ cookiecutter.ingest_name }}. Checking selections...")
    
    dataset_path = str(PROJECT_DIR / "config" / "dataset")
    if "{{ cookiecutter.data_standards }}" == "ACDD":
        os.remove(dataset_path + "-basic.yaml")
        os.remove(dataset_path + "-ioos.yaml")
        os.rename(dataset_path + "-acdd.yaml", dataset_path + ".yaml")
    elif "{{ cookiecutter.data_standards }}" == "IOOS":
        os.remove(dataset_path + "-basic.yaml")
        os.remove(dataset_path + "-acdd.yaml")
        os.rename(dataset_path + "-ioos.yaml", dataset_path + ".yaml")
    else:
        os.remove(dataset_path + "-acdd.yaml")
        os.remove(dataset_path + "-ioos.yaml")
        os.rename(dataset_path + "-basic.yaml", dataset_path + ".yaml")

    if "{{ cookiecutter.use_custom_data_reader }}" == "no":  # type: ignore
        os.remove(PROJECT_DIR / "readers.py")

    if "{{ cookiecutter.use_custom_data_converter }}" == "no":  # type: ignore
        os.remove(PROJECT_DIR / "converters.py")

    if "{{ cookiecutter.use_custom_qc }}" == "no":  # type: ignore
        os.remove(PROJECT_DIR / "qc.py")
        os.remove(PROJECT_DIR / "config" / "quality.yaml")

    if black := shutil.which("black"):
        subprocess.run([black, str(PROJECT_DIR)], check=True)

    print(f"Generated {{ cookiecutter.ingest_name }} in {PROJECT_DIR}")


if __name__ == "__main__":
    main()
    sys.exit(0)
