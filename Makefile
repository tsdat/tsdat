.PHONY: test
coverage:
	@ coverage run -m pytest
	@ coverage html
	@ echo "Run 'open htmlcov/index.html' to open it in your browser."

.PHONY: docs
docs:
	@ mkdocs serve
