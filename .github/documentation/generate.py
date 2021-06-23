import glob
import sys
from pathlib import Path

from lark import Lark, Transformer, Visitor, v_args, Transformer


class Function:
    def __init__(self, name: str, arguments, returntype: str):
        self.name = name
        self.arguments = arguments
        self.returntype = returntype

    def __str__(self):
        if self.name.startswith("__"):
            return None
        if self.arguments is None:
            self.arguments = []
        return "\n### {}({}) ⇒ `{}`".format(
            self.name, ", ".join(self.arguments), self.returntype
        )


def get_deco(decorators, key):
    if decorators and key in decorators:
        return decorators[key]
    return None


class SQLTransformer(Transformer):
    def start(self, statements):
        # flatten
        flat = [item for sublist in statements for item in sublist]
        return flat

    @v_args(inline=True)
    def custom_decorated_statement(self, decorators, statement):
        text = str(statement)
        availability = get_deco(decorators, "availability")
        if get_deco(decorators, "internal") or get_deco(decorators, "deprecated"):
            return ""
        if availability:
            text += f"\n*Since v{availability}*"
        text += "\n\n"
        return text

    def custom_decorators(self, lines):
        decorators = {}
        for line in lines:
            try:
                key, value = line.split(":")
                decorators[str(key).lower()] = value.strip()
            except:
                decorators[str(line).lower()] = True
        return decorators

    def custom_md_statement(self, children):
        return children

    # -- MARKDOWN --------------------------------------------------------------
    @v_args(inline=True)
    def custom_markdown(self, line=""):
        return "" + line

    # -- CREATE TYPE -----------------------------------------------------------
    def create_type_stmt(self, children):
        return ""

    # -- CREATE CAST -----------------------------------------------------------
    @v_args(inline=True)
    def create_cast_stmt(self, source, target, *children):
        return f"### `{source}` :: `{target}`"

    # -- CREATE OPERATOR -------------------------------------------------------
    @v_args(inline=True)
    def create_oper_stmt(self, name, options):
        return f"### Operator: `{options['LEFTARG']}` {name} `{options['RIGHTARG']}`"

    def create_oper_opts(self, opts):
        options = {}
        for [k, v] in opts:
            options[k] = v
        return options

    @v_args(inline=True)
    def create_oper_opt(self, option, value=None):
        return [str(option), value]

    # -- CREATE FUNCTION -------------------------------------------------------
    @v_args(inline=True)
    def create_func_stmt(self, name: str, arguments, returntype, *opts):
        # skip internal functions
        if name.startswith("__"):
            return None

        # print("func")

        return Function(name, arguments, returntype)

    def argument_list(self, children):
        return children

    # -- CREATE COMMENT --------------------------------------------------------
    @v_args(inline=True)
    def comment_on_stmt(self, child, text):
        child["type"] = "comment"
        child["text"] = text
        # print(child)
        return text

    # ... ON CAST
    @v_args(inline=True)
    def comment_on_cast(self, source, target):
        return {"on": "cast", "source": source, "target": target}

    # ... ON FUNCTION
    @v_args(inline=True)
    def comment_on_function(self, name, arguments):
        return {"on": "function", "name": name, "arguments": arguments}

    # ... ON OPERATOR
    @v_args(inline=True)
    def comment_on_operator(self, name, left, right):
        return {"on": "operator", "name": name, "left": left, "right": right}

    # -- SIMPLE RULES ----------------------------------------------------------

    true = lambda self, _: "`true`"
    false = lambda self, _: "`false`"
    number = v_args(inline=True)(int)

    @v_args(inline=True)
    def string(self, s):
        return s[1:-1].replace('\\"', '"')

    def fun_name(self, children):
        return children[1]

    def datatype(self, children):
        return children[0]

    @v_args(inline=True)
    def argument(self, name, argtype, default=None):
        out = ""
        if name:
            out += name + " "
        out += "`{}`".format(argtype)
        if default:
            out = "[{} = {}]".format(out, default)
        return out

    # -- TERMINALS -------------------------------------------------------------

    def SIGNED_NUMBER(self, children):
        return int(children)

    def CNAME(self, cname):
        return str(cname)

    def OPERATOR(self, name):
        return str(name)


"""
The concept:

We don't want to manually keep API docs in sync with SQL files.

So instead we should generate the docs from the sql/install/*.sql files.

We have the following things to document:
    * Type
    * Cast (put in same section as the type)
    * Operator
    * Function

Some functions do not need docs, we can signal this by not applying a comment.
This is internal operator functions for example.

We should parse the sql files, extract the above, and then document all things that
have comments applied.

We should probably hardcode a list of things that should not be documented, so we can
fail if new function are undocumented.

"""


if __name__ == "__main__":
    here = Path(__file__).parent

    parser = Lark.open(
        here / "sql.lark",
        parser="lalr",
        maybe_placeholders=True,
    )

    files = glob.glob(sys.argv[1])
    markdown = "# API Reference\n\n"

    for file in sorted(files):
        with open(file) as f:
            sql = f.read()
            parse_tree = parser.parse(sql)
            statements = SQLTransformer(visit_tokens=True).transform(parse_tree)
            markdown += "\n".join([str(stmt) for stmt in statements])

    print(markdown)
