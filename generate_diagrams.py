#!/usr/bin/env python3
"""Generate architecture diagrams for the project using various tools."""
import ast
import subprocess
import sys
from pathlib import Path


def check_tool(tool_name):
    """Check if a tool is installed"""
    try:
        subprocess.run([tool_name, "--version"], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def install_tools():
    """Install required tools"""
    print("Installing diagram generation tools...")
    tools = ["graphviz", "pydeps", "pylint"]

    for tool in tools:
        print(f"Installing {tool}...")
        subprocess.run([sys.executable, "-m", "pip", "install", tool], check=True)


def generate_pydeps_diagram():
    """Generate dependency graph using pydeps"""
    print("\nGenerating dependency graph with pydeps...")
    output_dir = Path("docs/diagrams")
    output_dir.mkdir(parents=True, exist_ok=True)

    subprocess.run([
        "pydeps", "src",
        "--max-bacon", "2",
        "--cluster",
        "--rankdir", "TB",
        "-o", str(output_dir / "dependencies.svg"),
        "--no-show"
    ], check=False)

    subprocess.run([
        "pydeps", "src/core",
        "--max-bacon", "3",
        "-o", str(output_dir / "core_dependencies.svg"),
        "--no-show"
    ], check=False)

    print(f"Dependency diagrams saved to {output_dir}")


def generate_pyreverse_diagrams():
    """Generate UML diagrams using pyreverse"""
    print("\nGenerating UML diagrams with pyreverse...")
    output_dir = Path("docs/diagrams")
    output_dir.mkdir(parents=True, exist_ok=True)

    subprocess.run([
        "pyreverse",
        "-o", "svg",
        "-p", "AirQuality",
        "--colorized",
        "src/domain",
        "-d", str(output_dir)
    ], check=False)

    subprocess.run([
        "pyreverse",
        "-o", "svg",
        "-p", "AirQualityPackages",
        "--colorized",
        "-k",
        "src",
        "-d", str(output_dir)
    ], check=False)

    print(f"UML diagrams saved to {output_dir}")


def extract_imports(file_path):
    """Extract imports from a Python file"""
    imports = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            tree = ast.parse(f.read())

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.append(node.module)
    except (SyntaxError, FileNotFoundError):
        pass
    return imports


def generate_import_graph():
    """Generate import graph using custom script"""
    print("\nGenerating import graph...")

    try:
        import graphviz
    except ImportError:
        print("Graphviz module not available, skipping import graph")
        return

    graph = graphviz.Digraph('imports', format='svg')
    graph.attr(rankdir='LR')

    modules = {}
    relationships = []

    for py_file in Path('src').rglob('*.py'):
        if '__pycache__' in str(py_file):
            continue

        module_name = str(py_file.relative_to('src')).replace('/', '.').replace('.py', '')
        if module_name.endswith('.__init__'):
            module_name = module_name[:-9]

        modules[module_name] = py_file
        imports = extract_imports(py_file)

        for imp in imports:
            if imp.startswith('src.'):
                relationships.append((module_name, imp[4:]))

    for module in modules:
        parts = module.split('.')
        if len(parts) > 1:
            graph.node(module, label=parts[-1])
        else:
            graph.node(module)

    for src, dst in relationships:
        if dst in modules:
            graph.edge(src, dst)

    output_dir = Path("docs/diagrams")
    output_dir.mkdir(parents=True, exist_ok=True)
    graph.render(output_dir / 'import_graph', cleanup=True)
    print(f"Import graph saved to {output_dir}/import_graph.svg")


def main():
    """Main function"""
    print("Air Quality Project - Architecture Diagram Generator")
    print("=" * 50)

    if not check_tool("dot"):
        print("Graphviz not found. Please install it:")
        print("  - macOS: brew install graphviz")
        print("  - Ubuntu: sudo apt-get install graphviz")
        print("  - Windows: Download from https://graphviz.org/download/")
        return

    try:
        import pydeps  # noqa: F401
        import graphviz  # noqa: F401
    except ImportError:
        install_tools()

    try:
        generate_pydeps_diagram()
    except RuntimeError as e:
        print(f"Error generating pydeps diagram: {e}")

    try:
        generate_pyreverse_diagrams()
    except RuntimeError as e:
        print(f"Error generating pyreverse diagrams: {e}")

    try:
        generate_import_graph()
    except RuntimeError as e:
        print(f"Error generating import graph: {e}")

    print("\nDiagram generation complete!")
    print("\nTo view the diagrams:")
    print("1. Open the Mermaid diagrams in ARCHITECTURE.md (renders on GitHub)")
    print("2. Check docs/diagrams/ for generated SVG files")


if __name__ == "__main__":
    main()