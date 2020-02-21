from odr.ovpn_config import OvpnClientConf, config_escape

def test_config_escape():
    # unchanged
    assert config_escape("test") == 'test'
    # quote if contains spaces
    assert config_escape("test 123") == '"test 123"'
    # escape quotes if quoting
    assert config_escape("test \" 123") == '"test \\" 123"'
    # escape backslash always
    assert config_escape("te\\st") == 'te\\\\st'
    assert config_escape("test te\\st") == '"test te\\\\st"'


def test_simple_conf():
    conf = OvpnClientConf()
    conf.add("test", "1", "2")
    conf.push("test", "1", "2")
    expected = 'test 1 2\npush "test 1 2"'
    assert conf.to_text() == expected
