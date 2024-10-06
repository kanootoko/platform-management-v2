"""Platform management v2 executable entrypoint is defined here."""

from pmv2.cli import main


if __name__ in ("__main__", "pmv2.__main__"):
    main()  # pylint: disable=no-value-for-parameter
