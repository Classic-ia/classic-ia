@echo off
REM ═══════════════════════════════════════════════════
REM Classic RH - Agendar Ingestor Atak no Windows
REM Executa a cada 30 minutos via Agendador de Tarefas
REM ═══════════════════════════════════════════════════

REM Criar tarefa agendada
schtasks /create /tn "ClassicRH_IngestorAtak" /tr "python C:\dados_atak\ingestor_atak.py" /sc minute /mo 30 /ru "%USERNAME%" /f

echo.
echo Tarefa "ClassicRH_IngestorAtak" criada com sucesso!
echo Frequencia: a cada 30 minutos
echo Script: C:\dados_atak\ingestor_atak.py
echo Log: C:\dados_atak\ingestor.log
echo.
echo Para verificar: schtasks /query /tn "ClassicRH_IngestorAtak"
echo Para remover:    schtasks /delete /tn "ClassicRH_IngestorAtak" /f
echo.
pause
