import time

from tqdm import tqdm

from nyborg_rpa.utils.pad import dispatch_pad_script


def pad_test(*, arg: int, should_raise: bool = False) -> dict[str, str]:

    for _ in tqdm(range(10), desc="Processing"):
        time.sleep(0.2)

    if should_raise:
        raise ValueError("This is a test error")

    return {"hello": "world", "foo": int(arg)}


if __name__ == "__main__":
    dispatch_pad_script(fn=pad_test)
