pythonExists()
{
  command -v python >/dev/null 2>&1
}

if pythonExists; then
    python -c 'import sys; print(".".join(str(x) for x in sys.version_info[0:2]))'
else
    echo ''
fi

