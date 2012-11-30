require 'helper'

class TestRayeux < Test::Unit::TestCase
  TEST_URL = 'http://localhost:8081/'

  def test_initializer_accepts_simple_url
    client = Rayeux::Client.new(TEST_URL)
    assert_equal(TEST_URL, client.instance_variable_get('@url'))
  end

  def test_initializer_accepts_url_via_options
    client = Rayeux::Client.new({:url => TEST_URL})
    assert_equal(TEST_URL, client.instance_variable_get('@url'))
  end

  def test_log_level_defaults_to_info
    client = Rayeux::Client.new({:url => TEST_URL})
    assert_equal('info', client.instance_variable_get('@log_level'))
  end

  def test_initializer_should_start_handshaking
    client = Rayeux::Client.new(TEST_URL)
    assert_equal('handshaking', client.instance_variable_get('@status'))
  end
end
