"""_compile_cached: lru_cache on compile() correctness."""


def test_same_source_returns_same_code_object(helper):
    # Same source string → cache hit → identical (==) code object.
    a = helper._compile_cached("x = 1")
    b = helper._compile_cached("x = 1")
    assert a is b


def test_different_sources_return_different_objects(helper):
    a = helper._compile_cached("x = 1")
    b = helper._compile_cached("x = 2")
    assert a is not b


def test_cache_info_tracks_hits_and_misses(helper):
    helper._compile_cached.cache_clear()
    helper._compile_cached("y = 1")           # miss
    helper._compile_cached("y = 1")           # hit
    helper._compile_cached("y = 1")           # hit
    helper._compile_cached("z = 1")           # miss
    info = helper._compile_cached.cache_info()
    assert info.hits == 2
    assert info.misses == 2


def test_compiled_code_actually_runs(helper):
    code = helper._compile_cached("result = 2 + 2")
    ns: dict = {}
    exec(code, ns)
    assert ns["result"] == 4
