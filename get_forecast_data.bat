REM Edt Configuration here #####################################################
SET R_VER=R-3.5.1
SET PY_VER=Python37-32
REM ############################################################################

SET SA_DATA_DIR=%~dp0
SET _R_SCRIPT_EXE="C:\Program Files\R\%R_VER%\bin\x64\Rscript.exe"
SET _R_SCRIPT_EXE_FILENAME="Rscript.exe"
SET _PIP_EXE="%LOCALAPPDATA%\Programs\Python\%PY_VER%\Scripts\pip.exe"
SET _PY_EXE="%LOCALAPPDATA%\Programs\Python\%PY_VER%\python.exe"

REM ### Get Forecast
%_R_SCRIPT_EXE% "%SA_DATA_DIR%r_forecast\forecast_arima_asc.R"
TASKKILL /F /IM %_R_SCRIPT_EXE_FILENAME%
%_R_SCRIPT_EXE% "%SA_DATA_DIR%r_forecast\forecast_arima_dsc.R"

REM ### Output the prediction model
%_PY_EXE% "%SA_DATA_DIR%get_prediction_model_not_fullset.py"
exit
