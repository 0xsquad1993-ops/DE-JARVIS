from typing import Callable, Dict, Any


class ToolRegistry:
    """
    Central registry for JARVIS tools.

    The AI Brain decides WHAT needs to be done.

    The Tool Registry tells JARVIS:
        - Which tool exists
        - What the tool does
        - Which Python function executes it

    The Orchestrator uses this registry to execute workflows.
    """

    def __init__(self):
        self._tools: Dict[str, Dict[str, Any]] = {}

    # =========================================================
    # REGISTER
    # =========================================================

    def register(
        self,
        name: str,
        description: str,
        function: Callable,
        overwrite: bool = False,
    ):
        """
        Register a tool in JARVIS.

        Parameters:
            name:
                Unique tool name.

            description:
                Human/AI readable description.

            function:
                Python callable that executes the tool.

            overwrite:
                Allow replacing an existing tool.
        """

        if not name or not name.strip():
            raise ValueError(
                "Tool name cannot be empty."
            )

        if not callable(function):
            raise TypeError(
                f"Tool '{name}' must have a callable function."
            )

        name = name.strip().upper()

        if name in self._tools and not overwrite:
            raise ValueError(
                f"Tool '{name}' is already registered."
            )

        self._tools[name] = {
            "name": name,
            "description": description,
            "function": function,
        }

    # =========================================================
    # GET TOOL
    # =========================================================

    def get(self, name: str) -> Dict[str, Any]:
        """
        Get complete tool definition.
        """

        if not name:
            raise ValueError(
                "Tool name cannot be empty."
            )

        name = name.strip().upper()

        if name not in self._tools:
            raise KeyError(
                f"Tool not found: {name}"
            )

        return self._tools[name]

    # =========================================================
    # CHECK TOOL
    # =========================================================

    def has(self, name: str) -> bool:
        """
        Check whether a tool exists.
        """

        if not name:
            return False

        return name.strip().upper() in self._tools

    # =========================================================
    # LIST TOOLS
    # =========================================================

    def list_tools(self) -> list:
        """
        Return all registered tools.

        Useful for:
            - Debugging
            - AI Brain
            - Streamlit dashboard
            - Monitoring
        """

        return [
            {
                "name": tool["name"],
                "description": tool["description"],
            }
            for tool in self._tools.values()
        ]

    # =========================================================
    # TOOL COUNT
    # =========================================================

    def count(self) -> int:
        """
        Return number of registered tools.
        """

        return len(self._tools)

    # =========================================================
    # EXECUTE TOOL
    # =========================================================

    def execute(
        self,
        name: str,
        *args,
        **kwargs
    ):
        """
        Execute a registered tool.

        Example:

            registry.execute(
                "READ_CSV",
                "employees.csv"
            )
        """

        tool = self.get(name)

        function = tool["function"]

        try:

            return function(
                *args,
                **kwargs
            )

        except Exception as error:

            raise RuntimeError(
                f"Tool execution failed: {name} | {error}"
            ) from error

    # =========================================================
    # UNREGISTER
    # =========================================================

    def unregister(self, name: str):
        """
        Remove a registered tool.
        """

        if not name:
            raise ValueError(
                "Tool name cannot be empty."
            )

        name = name.strip().upper()

        if name not in self._tools:
            raise KeyError(
                f"Tool not found: {name}"
            )

        del self._tools[name]

    # =========================================================
    # CLEAR
    # =========================================================

    def clear(self):
        """
        Remove all registered tools.

        Mainly useful for testing.
        """

        self._tools.clear()

    # =========================================================
    # TOOL DEFINITIONS FOR AI
    # =========================================================

    def get_tool_descriptions(self) -> str:
        """
        Return tools in a format that can later be
        supplied to the JARVIS AI Brain.
        """

        if not self._tools:
            return "No tools are currently registered."

        lines = []

        for tool in self._tools.values():

            lines.append(
                f"- {tool['name']}: "
                f"{tool['description']}"
            )

        return "\n".join(lines)


# =============================================================
# GLOBAL JARVIS TOOL REGISTRY
# =============================================================

tool_registry = ToolRegistry()