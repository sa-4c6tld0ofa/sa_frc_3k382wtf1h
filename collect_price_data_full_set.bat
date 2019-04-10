REM Edt Configuration here #####################################################
SET R_VER=R-3.5.1
SET PY_VER=Python37-32
REM ############################################################################

SET SA_DATA_DIR=%~dp0
SET _R_SCRIPT_EXE="C:\Program Files\R\%R_VER%\bin\x64\Rscript.exe"
SET _PIP_EXE="%LOCALAPPDATA%\Programs\Python\%PY_VER%\Scripts\pip.exe"
SET _PY_EXE="%LOCALAPPDATA%\Programs\Python\%PY_VER%\python.exe"

START "" %_PY_EXE% "%SA_DATA_DIR%\sa_data_collection\core\collection_instr_fulldata_full_set.py"
exit
