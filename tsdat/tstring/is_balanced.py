def _is_balanced(template: str) -> bool:
    stack: list[str] = []
    for char in template:
        if char in "{[":
            stack.append("}" if char == "{" else "]")
        elif char in "}]":
            if not stack or char != stack.pop():
                return False
    return len(stack) == 0
