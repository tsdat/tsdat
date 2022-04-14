.PHONY: test
coverage:
	@ coverage run -m pytest
	@ coverage html
	@ open htmlcov/index.html
