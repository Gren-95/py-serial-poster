# Simple python app to post data from serial to db with deduplication

## Getting started

```python
pip install -r requirements.txt
```

## Build commands

```python
python -m PyInstaller --onefile --noconsole --name scanner scanner.py

````
or
```
python -m PyInstaller --onefile --noconfirm --clean --noconsole --name scanner scanner.py
```

## Freeze requirements

```python
pip freeze > requirements.txt
```
