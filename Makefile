.PHONY:: all build
all: build analyze

build: rebar3
	@rebar3 compile

# Development
.PHONY:: develop analyze
develop: rebar3
	@echo "Starting development mode"
	@echo ""
	@echo "Once you get to the Erlang shell, you can try the pgsql driver with:"
	@echo "    application:ensure_all_started(pgsql)."
	@echo "    pgsql_demo:main(\"select 1 + 1 as sum\", [])."
	@echo ""
	@echo ""
	@rebar3 as dev auto

analyze: rebar3
	@rebar3 dialyzer

# Clean
.PHONY:: clean distclean
clean: rebar3
	@rebar3 clean --all

distclean: clean
	$(RM) -R _build/
