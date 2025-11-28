class CppToPython:
    def __init__(self):
        self.tabs = 0
        self.block = ''
        self.current_block = []
        self.in_function = False
        self.function_name = ""
        self.python_lines = []

    def translate(self, cpp_file):
        self.python_lines = []
        temp_to_write = ""

        for line in cpp_file:
            line = line.strip()
            if not line:
                continue

            if "}" == line:
                if self.tabs > 0:
                    self.tabs -= 1
                if self.in_function and self.function_name:
                    self.in_function = False
                    self.function_name = ""
                continue

            if line == "{":
                self.tabs += 1
                continue

            if "using" in line or "#include" in line:
                continue

            indent = "    " * self.tabs

            if "for" in line:
                self.translate_for(line, indent)
                self.tabs += 1
                continue

            elif "else" in line and "if" in line:
                self.tabs -= 1
                indent = "    " * self.tabs
                self.translate_else_if(line, indent)
                self.tabs += 1
                continue

            elif "else" in line and not "if" in line:
                self.tabs -= 1
                indent = "    " * self.tabs
                self.translate_else(line, indent)
                self.tabs += 1
                continue

            elif "if" in line:
                self.translate_if(line, indent)
                self.tabs += 1
                continue

            elif any(type_val in line.split()[0] for type_val in ["void", "int", "float", "double", "string", "bool"]) and "(" in line and ")" in line and not line.endswith(";"):
                self.translate_function(line, indent)
                self.tabs += 1
                continue

            elif "cout" in line and temp_to_write == "":
                temp_to_write = self.translate_cout(line, temp_to_write, indent)

            elif "<<" in line and temp_to_write != "":
                temp_to_write = self.translate_cout_continuation(line, temp_to_write, indent)

            elif (any(type_val in line for type_val in
                    ["string", "int", "float", "double", "bool", "void"]) and "=" in line) and (temp_to_write == ""):
                self.translate_variables(line, indent)

            elif "=" in line and ";" in line:
                line = line.replace(";", "")
                self.add_line(f"{indent}{line}")

            elif "(" in line and ")" in line and line.endswith(";") and not any(
                    keyword in line for keyword in ["if", "for", "while"]):
                self.translate_function_call(line, indent)

            elif "return" in line:
                self.translate_return(line, indent)

        python_file = open("result.py", "w")
        python_file.write('\n'.join(self.python_lines))
        python_file.close()

    def add_line(self, line):
        self.python_lines.append(line)

    def translate_variables(self, line, indent):
        types = ["string", "int", "float", "double", "bool", "void", ";", "const"]
        if "," in line:
            for type_name in types:
                line = line.replace(type_name, "")
            variables = line.split(",")
            for var in variables:
                var_name, var_value = var.split("=")
                var_name = var_name.strip()
                var_value = var_value.strip()
                self.add_line(f"{indent}{var_name} = {var_value}")
        else:
            for type_name in types:
                line = line.replace(type_name, "")
            self.add_line(f"{indent}{line.replace(';', '').strip()}")

    def translate_cout(self, line, temp_to_write, indent):
        line = line.replace("endl", "'\\n'").strip()
        parts = [part.strip() for part in line.split("<<") if "cout" not in part]

        to_write = f"{indent}print("
        to_write += ", ".join(parts)

        if ";" in line:
            to_write = to_write.replace(";", "") + ")"
            self.add_line(to_write)
            return ""
        else:
            return to_write

    def translate_cout_continuation(self, line, temp_to_write, indent):
        line = line.replace("endl", "'\\n'").strip()
        parts = [part.strip() for part in line.split("<<")]

        to_write = temp_to_write + ", " + ", ".join(parts)

        if ";" in line:
            to_write = to_write.replace(";", "") + ")"
            self.add_line(to_write)
            return ""
        else:
            return to_write

    def translate_for(self, line, indent):
        line = line.replace("(", " ").replace(")", " ").replace("{", "").strip()
        parts = [part.strip() for part in line.split(";")]

        init = parts[0].replace("for", "").strip()
        condition = parts[1].strip()
        increment = parts[2].strip()

        for type_name in ["int", "float", "double"]:
            init = init.replace(type_name, "").strip()

        if "=" in init:
            var_name = init.split("=")[0].strip()
            start_value = init.split("=")[1].strip()
        else:
            var_name = init
            start_value = "0"

        if "<=" in condition:
            var, limit = condition.split("<=")
            var = var.strip()
            limit = limit.strip()
            self.add_line(f"{indent}for {var_name} in range({start_value}, {limit} + 1):")
        elif ">=" in condition:
            var, limit = condition.split(">=")
            var = var.strip()
            limit = limit.strip()
            self.add_line(f"{indent}for {var_name} in range({start_value}, {limit} + 1):")
        elif "<" in condition:
            var, limit = condition.split("<")
            var = var.strip()
            limit = limit.strip()
            self.add_line(f"{indent}for {var_name} in range({start_value}, {limit}):")
        elif ">" in condition:
            var, limit = condition.split(">")
            var = var.strip()
            limit = limit.strip()
            self.add_line(f"{indent}for {var_name} in range({start_value}, {limit}):")


    def translate_else_if(self, line, indent):
        line = line.replace("(", "").replace(")", "").replace("{", "").replace("}", "").strip()
        condition = line.replace("else if", "").strip()
        condition = condition.replace("&&", " and ").replace("||", " or ")
        self.add_line(f"{indent}elif {condition}:")

    def translate_else(self, line, indent):
        line = line.replace("{", "").replace("}", "").strip()
        self.add_line(f"{indent}else:")

    def translate_if(self, line, indent):
        line = line.replace("(", "").replace(")", "").replace("{", "").strip()
        condition = line.replace("if", "").strip()
        condition = condition.replace("&&", " and ").replace("||", " or ")
        self.add_line(f"{indent}if {condition}:")

    def translate_function(self, line, indent):
        return_types = ["void", "int", "float", "double", "string", "bool"]
        function_line = line

        for return_type in return_types:
            if function_line.startswith(return_type):
                function_line = function_line.replace(return_type, "").strip()
                break

        function_line = function_line.replace("{", "").strip()

        if "(" in function_line and ")" in function_line:
            func_name = function_line.split("(")[0].strip()
            params_part = function_line.split("(")[1].split(")")[0]

            params = []
            if params_part.strip():
                param_list = params_part.split(",")
                for param in param_list:
                    param = param.strip()
                    for param_type in return_types + ["const", "[", "]"]:
                        param = param.replace(param_type, "").strip()
                    param_name = param.split()[-1] if param.split() else param
                    params.append(param_name)

            self.in_function = True
            self.function_name = func_name
            self.add_line(f"{indent}def {func_name}({', '.join(params)}):")

    def translate_function_call(self, line, indent):
        line = line.replace(";", "").strip()
        self.add_line(f"{indent}{line}")

    def translate_return(self, line, indent):
        line = line.replace(";", "").strip()
        return_value = line.replace("return", "").strip()

        if return_value:
            self.add_line(f"{indent}return {return_value}")
        else:
            self.add_line(f"{indent}return")


def main():
    translator = CppToPython()
    cpp_file = open("input2.cpp")

    translator.translate(cpp_file)


if __name__ == "__main__":
    main()