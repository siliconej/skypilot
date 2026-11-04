"""Constants needed to start skylet, with no SkyPilot imports.

attempt_skylet.py loads this file by path, so that starting skylet doesn't run
sky/__init__.py, which takes seconds. Keep it free of `sky` imports.
sky/skylet/constants.py re-exports these names, which is where other code should
read them from.
"""

# The base directory for all SkyPilot runtime artifacts.
# Historically, we have always used $HOME, but we couldn't
# do that for Slurm, because $HOME typically points to a NFS
# mounted directory, which does not work well with SQLite.
# https://sqlite.org/faq.html#q5
# Additionally, having the skypilot-runtime python venv be
# on an NFS makes things very slow.
SKY_RUNTIME_DIR = '${SKY_RUNTIME_DIR:-$HOME}'
# Same as above but for use within python code instead of shell commands.
# Example usage:
# os.path.join(
#    os.path.expanduser(os.environ.get(SKY_RUNTIME_DIR_ENV_VAR_KEY, '~')),
#    '.sky/jobs.db')
SKY_RUNTIME_DIR_ENV_VAR_KEY = 'SKY_RUNTIME_DIR'

SKY_UNSET_PYTHONPATH_AND_SET_CWD = 'env -u PYTHONPATH -C $HOME'
# We store the absolute path of the python executable (/opt/conda/bin/python3)
# in this file, so that any future internal commands that need to use python
# can use this path. This is useful for the case where the user has a custom
# conda environment as a default environment, which is not the same as the one
# used for installing SkyPilot runtime (ray and skypilot).
SKY_PYTHON_PATH_FILE = f'{SKY_RUNTIME_DIR}/.sky/python_path'
SKY_GET_PYTHON_PATH_CMD = (
    f'[ -s {SKY_PYTHON_PATH_FILE} ] && '
    f'cat {SKY_PYTHON_PATH_FILE} 2> /dev/null || '
    # POSIX builtin, present even when the `which` binary
    # is not (e.g. minimal RHEL/Rocky images ship no which).
    'command -v python3')
# Python executable, e.g., /opt/conda/bin/python3
SKY_PYTHON_CMD = (f'{SKY_UNSET_PYTHONPATH_AND_SET_CWD} '
                  f'$({SKY_GET_PYTHON_PATH_CMD})')

SKYPILOT_ENV_VAR_PREFIX = 'SKYPILOT_'

SKYLET_VERSION = '44'  # job timestamp responses carry an exact double.
SKYLET_VERSION_FILE = '.sky/skylet_version'
SKYLET_LOG_FILE = '.sky/skylet.log'
SKYLET_PID_FILE = '.sky/skylet_pid'
SKYLET_PORT_FILE = '.sky/skylet_port'
# The Slurm skylet keeper consumes this start spec.
SKYLET_START_FILE = '.sky/skylet_start'
SKYLET_GRPC_PORT = 46590
# Set on a Kubernetes hostNetwork pod: the port the server assigned to skylet
# (host_network_probe's 'skylet' slot) and will dial.
SKYLET_PORT_ENV_VAR = 'SKYPILOT_SKYLET_PORT'
