import subprocess
from research import nba_points_v3 as v3

def test_optional_evidence_timeout_keeps_base_path_available(monkeypatch):
 def failed(*args,**kwargs):raise subprocess.TimeoutExpired(args[0],180)
 monkeypatch.setattr(v3.subprocess,'run',failed)
 r=v3.optional_capture(['python','-m','ingest.nba_rosters'],180)
 assert r.returncode==124 and r.stdout=='' and r.stderr

def test_optional_evidence_os_failure_is_explicit(monkeypatch):
 def failed(*args,**kwargs):raise OSError('collector unavailable')
 monkeypatch.setattr(v3.subprocess,'run',failed)
 r=v3.optional_capture(['collector'],180)
 assert r.returncode==124 and 'unavailable' in r.stderr
