.PHONY : clean comments delete delete-apply delete-comments import import-apply init initialize lint setup setup-dev test

clean:
	./clean

comments: setup
	pipenv run python add_external_id_comment.py

delete:
	pipenv run python delete_imported_entities.py

delete-apply:
	pipenv run python delete_imported_entities.py --apply

delete-comments: setup
	pipenv run python delete_comments.py

import: initialize
	pipenv run python pivotal_import.py

import-apply: initialize
	pipenv run python pivotal_import.py --apply

init:
	pipenv run python initialize.py

initialize: setup init

lint:
	./lint

setup:
	./setup

setup-dev:
	pipenv install --dev

test:
	./test
