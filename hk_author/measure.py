import socket,struct,subprocess,time,json,shutil
from pathlib import Path
TMP=Path('/tmp/codex-hk9-tmp')
I=lambda n:struct.pack('!i',n)
H=lambda n:struct.pack('!h',n)
def msg(s,t,b):s.sendall(t+I(len(b)+4)+b)
def recv(s,n):
 b=b''
 while len(b)<n:
  x=s.recv(n-len(b))
  if not x: raise EOFError()
  b+=x
 return b
def drain(s):
 out=[]
 while True:
  t=recv(s,1);b=recv(s,struct.unpack('!i',recv(s,4))[0]-4)
  if t==b'Z':return out
  if t==b'E':out.append({'error':b.decode().split('\0')})
  if t==b't':out.append({'parameters':list(struct.unpack('!'+str(struct.unpack('!h',b[:2])[0])+'i',b[2:]))})
  if t==b'T':
   cols=[];p=2
   for _ in range(struct.unpack('!h',b[:2])[0]):
    e=b.index(0,p);name=b[p:e].decode();p=e+1
    table,attr,oid,size,mod,fmt=struct.unpack('!ihihih',b[p:p+18]);p+=18;cols.append([name,oid,mod])
   out.append({'columns':cols})
  if t==b'D':
   row=[];p=2
   for _ in range(struct.unpack('!h',b[:2])[0]):
    n=struct.unpack('!i',b[p:p+4])[0];p+=4;row.append(None if n<0 else b[p:p+n].decode());p+=max(n,0)
   out.append({'row':row})
  if t==b'C':out.append({'command':b[:-1].decode()})

def measure(name, steps):
 sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1];sock.close()
 store=TMP/('seamb-'+name+'-'+str(time.time_ns()))
 with (TMP/('seamb-'+name+'.log')).open('w') as log:
  p=subprocess.Popen([str(TMP/'wadjet-tip'),'serve','--storage-type=file','--data-dir='+str(store),'--pg-addr=127.0.0.1:'+str(port),'--http-addr=127.0.0.1:0','--metrics-addr=127.0.0.1:0','--background-compaction=false'],stdout=log,stderr=log)
  try:
   for _ in range(100):
    try:s=socket.create_connection(('127.0.0.1',port),timeout=30);break
    except OSError:time.sleep(.05)
   else:raise RuntimeError('server did not start')
   b=I(196608)+b'user\0wadjet\0database\0wadjet\0\0';s.sendall(I(len(b)+4)+b);drain(s)
   out=[]
   for step in steps:
    if isinstance(step,str):msg(s,b'Q',step.encode()+b'\0');res=drain(s)
    else:
     sql,oids,vals=step
     msg(s,b'P',b'\0'+sql.encode()+b'\0'+H(len(oids))+b''.join(I(i) for i in oids));msg(s,b'D',b'S\0');msg(s,b'S',b'');res=drain(s)
     if not any('error' in r for r in res):
      msg(s,b'B',b'\0\0'+H(0)+H(len(vals))+b''.join((I(-1) if v is None else I(len(v.encode()))+v.encode()) for v in vals)+H(0));msg(s,b'E',b'\0'+I(0));msg(s,b'S',b'');res+=drain(s)
    out.append({'sql':step,'answer':res})
   s.close();return out
  finally:p.terminate();p.wait(timeout=10)
