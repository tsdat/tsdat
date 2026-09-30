from pathlib import Path


def test_all_pipelines_define_tests():

    excludes = Path(".gitignore").read_text().splitlines()

    for module in Path("pipelines/").iterdir():
        if not module.is_dir() or module.name in excludes:
            continue

        test_module = module / "test"
        no_test_dir_msg = f"Module {module.as_posix()} does not have a test module!"
        f" Please create {(module / 'test' / 'test_pipeline.py').as_posix()} and add at"
        " least one test. Please also ensure that an empty __init__.py file is created"
        " within the test module."
        assert test_module.is_dir(), no_test_dir_msg

        tests = list(test_module.glob("test_*[.]py"))
        no_test_scrips_msg = f"No test scrips exist for module {module.as_posix()}!"
        " Please add at least one test script for this module."
        assert len(tests), no_test_scrips_msg

        init = list(test_module.glob("__init__[.]py"))
        no_init_msg = f"No __init__.py file exists for module {module.as_posix()}."
        " Please add this to aid pytest's discovery of your module's tests."
        assert len(init), no_init_msg
