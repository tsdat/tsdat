import yaml
import imp


with open("data/code_example.yaml", "r") as file:
    dict = yaml.load(file)

code = dict['code']
mymodule = imp.new_module('mymodule')
exec(code, mymodule.__dict__)

end_day = mymodule.get_end_day(1, 1, 2021)
print(end_day)



