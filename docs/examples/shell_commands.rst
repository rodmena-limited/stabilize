Shell Commands
==============

This example shows how to chain shell commands, passing output from one to the next using substitution placeholders.

.. code-block:: python

    # Stage 1: Generate
    context={"command": "echo 'hello world'"}

    # Stage 2: Process (uses output from Stage 1)
    context={"command": "echo '{stdout}' | tr 'a-z' 'A-Z'"}

See ``examples/shell-example.py`` for the full runnable code.
