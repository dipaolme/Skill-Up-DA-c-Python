from jinja2 import Environment, FileSystemLoader
import yaml
import os

file_dir = os.path.dirname(os.path.abspath(__file__))
env = Environment(loader=FileSystemLoader(file_dir))

tempate = env.get_or_select_template('Temp_UNDag.jinja2')
dag_name = '_dag_elt'
for filename in os.listdir(file_dir):
    if filename.endswith(".yaml"):
        with open(f"{file_dir}/{filename}",'r') as cong:
            config = yaml.safe_load(cong)
            with open(f"dags/{config['name_dag']}{dag_name}.py", "w") as f:
                f.write(tempate.render(config))