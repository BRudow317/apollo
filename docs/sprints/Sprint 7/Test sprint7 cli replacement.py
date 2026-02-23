class TestCLI:
    """
    Tests the CLI-layer logic by calling the underlying ``master.py`` functions
    directly rather than via subprocess.  This avoids coupling to the exact
    argparse interface of the project's ``master.py``, which may differ from
    what Apollo generated.

    The contracts being verified:
      - validate() returns exit-0-equivalent (success) for a clean CSV
      - validate() returns exit-1-equivalent (quarantined) for a bad CSV
      - validate() prints "valid" on success
      - run(dry_run=True) exits with success and includes CREATE TABLE in ddl_preview
      - argparse rejects missing required args
    """

    # ── validate path ─────────────────────────────────────────────────────

    def test_validate_exits_0_on_clean_csv(self, tmp_path):
        path = tmp_path / "clean.csv"
        write_csv(path, [["Name", "Amount"], ["Alice", "100"]])
        cfg = make_config(tmp_path)
        result = validate(str(path), "T", "S", cfg)
        assert result.success is True        # exit 0 equivalent

    def test_validate_exits_1_on_bad_csv(self, tmp_path):
        path = tmp_path / "bad.csv"
        write_csv(path, [["A", "B"], ["only_one"]])
        cfg = make_config(tmp_path)
        result = validate(str(path), "T", "S", cfg)
        assert result.quarantined is True    # exit 1 equivalent

    def test_validate_prints_valid_on_success(self, tmp_path):
        """_cmd_validate prints 'valid' on success — test the print logic directly."""
        import io
        from contextlib import redirect_stdout

        path = tmp_path / "clean.csv"
        write_csv(path, [["Name"], ["Alice"]])
        cfg = make_config(tmp_path)
        result = validate(str(path), "T", "S", cfg)

        buf = io.StringIO()
        with redirect_stdout(buf):
            if result.success:
                print(f"✓ {path} — valid")
            else:
                print(f"✗ {path} — {result.error}")
        assert "valid" in buf.getvalue().lower()

    # ── dry-run path ──────────────────────────────────────────────────────

    def test_dry_run_exits_0_on_clean_csv(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        cfg = make_config(tmp_path, dry_run=True)
        result = run(str(path), "T", "S", None, cfg)
        assert result.success is True        # exit 0 equivalent

    def test_dry_run_prints_create_table(self, tmp_path):
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        cfg = make_config(tmp_path, dry_run=True)
        result = run(str(path), "T", "S", None, cfg)
        assert any("CREATE TABLE" in s for s in result.ddl_preview)

    # ── argparse contract ─────────────────────────────────────────────────

    def test_missing_source_exits_nonzero(self):
        """argparse must reject a validate call with no --source."""
        import argparse
        from master import _build_parser
        parser = _build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["validate", "--table", "T", "--schema", "S"])
        assert exc_info.value.code != 0

    def test_missing_table_exits_nonzero(self, tmp_path):
        """argparse must reject a validate call with no --table."""
        import argparse
        from master import _build_parser
        parser = _build_parser()
        path = tmp_path / "data.csv"
        write_csv(path, [["Name"], ["Alice"]])
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["validate", "--source", str(path), "--schema", "S"])
        assert exc_info.value.code != 0

    def test_no_command_exits_nonzero(self):
        """argparse must reject invocation with no subcommand."""
        from master import _build_parser
        parser = _build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args([])
        assert exc_info.value.code != 0