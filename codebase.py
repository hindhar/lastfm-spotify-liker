import os
import fnmatch

def should_ignore_for_structure(path, ignore_patterns):
    return any(fnmatch.fnmatch(path, pattern) for pattern in ignore_patterns)

def should_ignore_for_content(path, ignore_patterns, script_name, output_file):
    return any(fnmatch.fnmatch(path, pattern) for pattern in ignore_patterns) or \
           path == script_name or path == output_file

def generate_codebase_txt(root_dir, output_file, script_name):
    structure_ignore_patterns = [
        '*venv*',  # Virtual environment
        '__pycache__',  # Python cache
        'node_modules',  # Node.js modules
        'build',  # Build directories
        'dist',  # Distribution directories
        '.git',  # Git repository folder
    ]

    content_ignore_patterns = [
        '.*',  # Hidden files and directories
        '*venv*',  # Virtual environment
        '*.pyc',  # Python bytecode
        '*.pyo',  # Python optimized bytecode
        '*.pyd',  # Python DLL file
        '__pycache__',  # Python cache
        '*.log',  # Log files
        '*.swp',  # Vim swap files
        '*.swo',  # Vim swap files
        '*.swn',  # Vim swap files
        '*.DS_Store',  # macOS system files
        'node_modules',  # Node.js modules
        'build',  # Build directories
        'dist',  # Distribution directories
        '*.egg-info',  # Python egg info
        '*.egg',  # Python eggs
        '*.so',  # Shared libraries
        '*.dll',  # Windows DLL files
        '.git',  # Git repository folder
    ]

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("Directory Structure:\n")
        f.write("====================\n\n")

        for root, dirs, files in os.walk(root_dir):
            dirs[:] = [d for d in dirs if not should_ignore_for_structure(d, structure_ignore_patterns)]
            files = [file for file in files if not should_ignore_for_structure(file, structure_ignore_patterns)]

            level = root.replace(root_dir, '').count(os.sep)
            indent = ' ' * 4 * level
            f.write(f'{indent}{os.path.basename(root)}/\n')
            sub_indent = ' ' * 4 * (level + 1)
            for file in files:
                f.write(f'{sub_indent}{file}\n')

        f.write("\n\nFile Contents:\n")
        f.write("==============\n\n")

        for root, dirs, files in os.walk(root_dir):
            dirs[:] = [d for d in dirs if not should_ignore_for_content(d, content_ignore_patterns, script_name, output_file)]
            files = [file for file in files if not should_ignore_for_content(file, content_ignore_patterns, script_name, output_file)]

            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, root_dir)
                f.write(f"File: {relative_path}\n")
                f.write("=" * (len(relative_path) + 6) + "\n\n")
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as source_file:
                        content = source_file.read()
                        f.write(content)
                except Exception as e:
                    f.write(f"Error reading file: {str(e)}\n")
                
                f.write("\n\n")

if __name__ == "__main__":
    current_dir = os.getcwd()
    output_file = "codebase.txt"
    script_name = os.path.basename(__file__)
    generate_codebase_txt(current_dir, output_file, script_name)
    print(f"Codebase information has been written to {output_file}")