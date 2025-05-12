from nyborg_rpa.utils.pad_script import dispatch_pad_script


def foo(number: int) -> int:
    print(f"Hello from foo with number: {number}")

    return 42 + number


if __name__ == "__main__":
    print("Hello, world!")
    dispatch_pad_script(fn=foo)
