<<<<<<< HEAD
from jinja2 import Environment, FileSystemLoader
import yaml
import os

file_dir = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_or_select_template('dag_template.jinja2')

for filename in os.listdir(file_dir):
    if filename.endswith(".yaml"):
        with open(f"{file_dir}/{filename}", "r") as configfile:
            config = yaml.safe_load(configfile)
            with open(f"dags/{config['dag_id']}.py", "w") as f:
=======
from jinja2 import Environment, FileSystemLoader
import yaml
import os

file_dir = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_or_select_template('dag_template.jinja2')

for filename in os.listdir(file_dir):
    if filename.endswith(".yaml"):
        with open(f"{file_dir}/{filename}", "r") as configfile:
            config = yaml.safe_load(configfile)
            with open(f"dags/{config['dag_id']}.py", "w") as f:
>>>>>>> cd481ad119f5296390759ecda4e6d99915d9938f
                f.write(template.render(config))