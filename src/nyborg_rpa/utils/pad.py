import contextlib
import inspect
import json
import sys
from functools import wraps
from typing import Callable

import argh


def dispatch_pad_script(fn: Callable) -> None:
    """
    Wrap and dispatch a function `fn` with an additional `pad_script` argument using `argh`, which
    can redirect the output to stderr and output the result as JSON if `pad_script=True`.
    """

    pad_param = inspect.Parameter(
        name="pad_script",
        kind=inspect.Parameter.KEYWORD_ONLY,
        default=False,
        annotation=bool,
    )

    @wraps(fn)
    @argh.arg(
        "--pad-script",
        help="Use with Power Automate Desktop",
        default=pad_param.default,
    )
    def wrapped_fn(*args, pad_script: bool = pad_param.default, **kwargs):

        if pad_script:
            with contextlib.redirect_stdout(new_target=sys.stderr):
                res = fn(*args, **kwargs)
                out = json.dumps(res)

        else:
            out = fn(*args, **kwargs)

        return out

    # update the function signature so that argh can parse it correctly
    sig = inspect.signature(fn)
    wrapped_fn.__signature__ = sig.replace(
        parameters=[*sig.parameters.values(), pad_param],
    )

    # dispatch the command with argh
    argh.dispatch_command(
        function=wrapped_fn,
        old_name_mapping_policy=False,
    )
