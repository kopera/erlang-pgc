.PHONY:: all build
all: build analyze

build:
	@rebar3 compile

# Development
.PHONY:: develop analyze
develop:
	@echo "Starting development mode"
	@echo ""
	@echo "Once you get to the Erlang shell, you can try the pgsql driver with:"
	@echo "    pgsql_demo:execute(\"select 1 + 1 as sum\", [])."
	@echo ""
	@echo ""
	@rebar3 as dev auto

analyze:
	@rebar3 dialyzer

.PHONY:: test test-units test-system
test: test-units test-system

test-units:
	@rebar3 eunit

test-system:
	@rebar3 ct

# Clean
.PHONY:: clean distclean
clean:
	@rebar3 clean --all

distclean: clean
	$(RM) -R _build/
