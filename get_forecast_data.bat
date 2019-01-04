REM Edt Configuration here #####################################################
SET R_VER=R-3.5.1
SET PY_VER=Python37-32
REM ############################################################################

SET SA_DATA_DIR=%~dp0
SET _R_SCRIPT_EXE="C:\Program Files\R\%R_VER%\bin\x64\Rscript.exe"
SET _PIP_EXE="%LOCALAPPDATA%\Programs\Python\%PY_VER%\Scripts\pip.exe"
SET _PY_EXE="%LOCALAPPDATA%\Programs\Python\%PY_VER%\python.exe"

REM ### Get Forecast
START "" %_R_SCRIPT_EXE% "%SA_DATA_DIR%r_forecast\forecast_arima_asc.R"
ping 127.0.0.1 -n 30
START "" %_R_SCRIPT_EXE% "%SA_DATA_DIR%r_forecast\forecast_arima_dsc.R"
exit
