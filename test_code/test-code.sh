if command -v python3 &>/dev/null; then
  PYTHON="python3"
else
  PYTHON="python"
fi
echo ${PYTHON}

if python3 -V &>/dev/null; then
  PYTHON="python3"
else
  PYTHON="python"
fi

echo ${PYTHON}

# test="TEST"

# TCPPING="${PYTHON} ${test}/tcpping.py"
# echo ${TCPPING}

# TCPPING="ping"
# ${TCPPING} -c 1 google.com
# tcpping_error_code=$?
# echo ${tcpping_error_code}