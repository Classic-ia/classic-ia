@echo off
chcp 65001 >nul
title Renomeador de Documentos RH - Classic Couros
cd /d "%~dp0"

echo.
echo  ============================================================
echo   RENOMEADOR DE DOCUMENTOS RH - Classic Couros
echo  ============================================================
echo.

rem --- Procura o Python instalado (comando "python") ---
where python >nul 2>nul
if %errorlevel%==0 (
    echo  Python encontrado. Abrindo o programa...
    echo.
    python renomeador_rh.py
    goto fim
)

rem --- Tenta o lancador "py" (instalacoes mais novas) ---
where py >nul 2>nul
if %errorlevel%==0 (
    echo  Python encontrado. Abrindo o programa...
    echo.
    py renomeador_rh.py
    goto fim
)

rem --- Python nao encontrado ---
echo  ------------------------------------------------------------
echo   O PYTHON NAO ESTA INSTALADO NESTE COMPUTADOR.
echo  ------------------------------------------------------------
echo.
echo   Vou abrir o site oficial para voce baixar o Python.
echo.
echo   IMPORTANTE: na tela de instalacao, MARQUE a opcao
echo   "Add Python to PATH" (Adicionar Python ao PATH)
echo   antes de clicar em "Install".
echo.
echo   Depois de instalar, feche esta janela e abra o
echo   INICIAR.bat novamente.
echo.
start https://www.python.org/downloads/
pause
goto sair

:fim
if %errorlevel% neq 0 (
    echo.
    echo  ------------------------------------------------------------
    echo   Ocorreu um problema ao executar o programa.
    echo   Anote a mensagem acima e avise o suporte de TI.
    echo  ------------------------------------------------------------
    echo.
    pause
)

:sair
