from odr.ovpn import determine_daemon_name


def test_daemon_name_env(mocker):
    mocker.patch.dict("os.environ", {"daemon_name": "test_name"})
    assert determine_daemon_name("test-name_openvpn") == "test_name"


def test_daemon_name_argv(mocker):
    # normal case
    mocker.patch("sys.argv", ["/test/test_daemonname"])
    assert determine_daemon_name("test") == "daemonname"
    # two underscores
    mocker.patch("sys.argv", ["/test/test_daemonname_2"])
    assert determine_daemon_name("test") == "daemonname_2"
    # no underscores
    mocker.patch("sys.argv", ["/test/testdaemonname"])
    assert determine_daemon_name("test") is None
