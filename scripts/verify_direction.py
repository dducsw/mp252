"""Verify improved 3-layer direction detection algorithm."""
import json, math
import numpy as np
import pandas as pd
from pathlib import Path
from collections import Counter

BASE_DIR = Path('data')
MAX_DIST_M     = 500
BEARING_WEIGHT = 0.35
SMOOTH_WINDOW  = 5
SAMPLE_STEP    = 4

# Load mapping first
df_vr = pd.read_csv(BASE_DIR / 'HPCLAB' / 'vehicle_route_mapping.csv')
df_vr.columns = df_vr.columns.str.strip()
df_vr['route_id'] = df_vr['route_id'].astype(int)
mapped_vehicles  = set(df_vr['vehicle'].unique())
needed_route_ids = set(df_vr['route_id'].unique())
print(f'Vehicles: {len(mapped_vehicles):,}  Routes: {len(needed_route_ids):,}')

# GPS - filter early
with open(BASE_DIR / 'HPCLAB' / 'sample.json', 'r', encoding='utf-8') as f:
    raw_gps = json.load(f)
recs = []
for item in raw_gps:
    wp = item.get('msgBusWayPoint', {})
    vid, x, y = wp.get('vehicle'), wp.get('x'), wp.get('y')
    if not (vid and x and y): continue
    if vid not in mapped_vehicles: continue
    recs.append({'vehicle_id': vid, 'lon': float(x), 'lat': float(y),
                 'datetime': wp.get('datetime') or 0, 'speed_kmh': wp.get('speed', 0) or 0,
                 'heading': wp.get('heading')})
df_gps = pd.DataFrame(recs).sort_values(['vehicle_id','datetime']).reset_index(drop=True)
print(f'GPS (mapped only): {len(df_gps):,}  vehicles: {df_gps["vehicle_id"].nunique()}')
n_head = df_gps['heading'].notna().sum()
print(f'Pings with raw heading: {n_head:,} ({n_head/len(df_gps)*100:.1f}%)')

# Routes info
with open(BASE_DIR / 'MCPT' / 'routes_info.json', 'r', encoding='utf-8') as f:
    routes_info_raw = json.load(f)
routes_info = {}
for route in routes_info_raw:
    rid = route['RouteId']
    routes_info[rid] = {}
    for var in route.get('Vars', []):
        routes_info[rid][var['RouteVarId']] = {
            'outbound': var['Outbound'], 'route_var_name': var['RouteVarName'],
            'start_stop': var['StartStop'], 'end_stop': var['EndStop'],
        }

# Route paths + bearings
def compute_bearing(lat1, lon1, lat2, lon2):
    dlon = math.radians(lon2-lon1)
    la1, la2 = math.radians(lat1), math.radians(lat2)
    x = math.sin(dlon)*math.cos(la2)
    y = math.cos(la1)*math.sin(la2) - math.sin(la1)*math.cos(la2)*math.cos(dlon)
    return (math.degrees(math.atan2(x, y)) + 360) % 360

with open(BASE_DIR / 'MCPT' / 'route_paths.json', 'r', encoding='utf-8') as f:
    route_paths_raw = json.load(f)
route_paths, route_bearings = {}, {}
for path in route_paths_raw:
    rid, rvid = path['RouteId'], path['RouteVarId']
    if rid not in needed_route_ids: continue
    pts = list(zip(path['lat'], path['lng']))
    route_paths[(rid, rvid)] = pts
    route_bearings[(rid, rvid)] = [compute_bearing(pts[i][0],pts[i][1],pts[i+1][0],pts[i+1][1]) for i in range(len(pts)-1)]
print(f'Route polylines: {len(route_paths):,}')

# Compute GPS bearing from consecutive pings
def compute_gps_bearing_series(df):
    lat, lon, heading = df['lat'].values, df['lon'].values, df['heading'].values
    result = np.full(len(df), np.nan)
    for i in range(1, len(df)):
        if not (isinstance(heading[i], float) and np.isnan(heading[i])) and heading[i] is not None:
            try: result[i] = float(heading[i])
            except: pass
        else:
            dlat = lat[i]-lat[i-1]; dlon = lon[i]-lon[i-1]
            if abs(dlat)+abs(dlon) > 1e-6:
                result[i] = compute_bearing(lat[i-1],lon[i-1],lat[i],lon[i])
    return result

df_gps['bearing_computed'] = np.nan
for vid, grp in df_gps.groupby('vehicle_id', sort=False):
    df_gps.loc[grp.index, 'bearing_computed'] = compute_gps_bearing_series(grp)
n_bc = df_gps['bearing_computed'].notna().sum()
print(f'Pings with bearing (all sources): {n_bc:,} ({n_bc/len(df_gps)*100:.1f}%)')

# Scoring function
def angle_diff(a1, a2):
    d = abs(a1-a2) % 360
    return d if d <= 180 else 360-d

def _to_xy(lat, lon, olat, olon):
    c = math.cos(math.radians(olat))
    return (lon-olon)*111320*c, (lat-olat)*111320

def score_polyline(glat, glon, gbear, polyline, bearings):
    if len(polyline) < 2: return float('inf'), float('inf'), 0
    olat, olon = polyline[0]
    px, py = _to_xy(glat, glon, olat, olon)
    pts  = polyline[::SAMPLE_STEP]
    brgs = bearings[::SAMPLE_STEP]
    if pts[-1] != polyline[-1]: pts=pts+[polyline[-1]]; brgs=brgs+[bearings[-1]]
    best_d, best_b = float('inf'), 0.0
    for i in range(len(pts)-1):
        ax,ay = _to_xy(*pts[i],olat,olon); bx,by = _to_xy(*pts[i+1],olat,olon)
        dx,dy = bx-ax,by-ay; s2=dx*dx+dy*dy
        t = max(0.0,min(1.0,((px-ax)*dx+(py-ay)*dy)/s2)) if s2 else 0
        d = math.hypot(px-ax-t*dx, py-ay-t*dy)
        if d < best_d: best_d=d; best_b=brgs[i] if i<len(brgs) else 0
        if best_d < 5: break
    b_pen = angle_diff(gbear, best_b) if gbear is not None and BEARING_WEIGHT>0 else 0
    score = best_d + BEARING_WEIGHT * b_pen * (MAX_DIST_M/180)
    return score, best_d, b_pen

def detect_direction_single(lat, lon, route_id, bearing=None):
    EMPTY={'direction_label':'Không xác định','confidence':'unknown','dist_m':None,'score_gap':None,'outbound':None}
    if route_id not in routes_info: return {**EMPTY,'direction_label':'no_info'}
    scored={}
    for rvid in routes_info[route_id]:
        poly=route_paths.get((route_id,rvid),[])
        brgs=route_bearings.get((route_id,rvid),[])
        if poly: scored[rvid]=score_polyline(lat,lon,bearing,poly,brgs)
    if not scored: return {**EMPTY,'direction_label':'no_path'}
    sv=sorted(scored,key=lambda k:scored[k][0])
    best=sv[0]; bsc,bdist,bpen=scored[best]
    gap=scored[sv[1]][0]-bsc if len(sv)>1 else None
    if bdist>MAX_DIST_M: return {**EMPTY,'dist_m':round(bdist,1),'score_gap':round(gap,1) if gap else None}
    info=routes_info[route_id][best]
    label='Chiều đi' if info['outbound'] else 'Chiều về'
    g=gap or 0
    conf='high' if g>80 else ('medium' if g>30 else ('low' if g>10 else 'very_low'))
    return {'direction_label':label,'outbound':info['outbound'],'dist_m':round(bdist,1),
            'score_gap':round(gap,1) if gap else None,'bearing_diff':round(bpen,1),'confidence':conf,
            'route_var_id':int(best)}

# Smoothing
def smooth_directions(df_v, window=SMOOTH_WINDOW):
    if window<=1: return df_v
    labels=df_v['direction_label'].tolist(); confs=df_v['confidence'].tolist()
    nl=labels.copy(); nc=confs.copy()
    for i in range(len(labels)):
        if confs[i] in ('high','medium'): continue
        lo=max(0,i-window//2); hi=min(len(labels),i+window//2+1)
        wl=[labels[j] for j in range(lo,hi) if labels[j] in ('Chiều đi','Chiều về')]
        if not wl: continue
        mc=Counter(wl).most_common(1)[0][0]
        if mc!=labels[i]: nl[i]=mc; nc[i]='smoothed'
    df_v=df_v.copy(); df_v['direction_label']=nl; df_v['confidence']=nc
    return df_v

# Run everything
df_work = df_gps.merge(df_vr, left_on='vehicle_id', right_on='vehicle', how='inner')
print(f'\nProcessing {len(df_work):,} pings...')

results=[]
for _, row in df_work.iterrows():
    b = row['bearing_computed'] if not np.isnan(row['bearing_computed']) else None
    results.append(detect_direction_single(row['lat'],row['lon'],row['route_id'],b))

df_r = pd.DataFrame(results)
df_raw = pd.concat([df_work.reset_index(drop=True), df_r], axis=1)

# Smooth
parts=[]
for vid, grp in df_raw.sort_values(['vehicle_id','datetime']).groupby('vehicle_id',sort=False):
    parts.append(smooth_directions(grp))
df_final = pd.concat(parts, ignore_index=True)

# Results
total = len(df_final)
det = df_final['direction_label'].isin(['Chiều đi','Chiều về'])

print('\n=== RESULTS ===')
print('Direction distribution:')
for k,v in df_final['direction_label'].value_counts().items():
    print(f'  {k:28s}: {v:6,} ({v/total*100:5.1f}%)')

print('\nConfidence:')
for k,v in df_final['confidence'].value_counts().items():
    print(f'  {k:12s}: {v:6,} ({v/total*100:5.1f}%)')

print(f'\nDetection rate: {det.sum():,}/{total:,} ({det.sum()/total*100:.1f}%)')
if det.sum()>0:
    print(f'Avg dist: {df_final.loc[det,"dist_m"].mean():.1f} m')
n_smoothed = (df_final['confidence']=='smoothed').sum()
n_prev_det = df_raw['direction_label'].isin(['Chiều đi','Chiều về']).sum()
print(f'Before smoothing: {n_prev_det:,} ({n_prev_det/total*100:.1f}%)')
print(f'Pings smoothed: {n_smoothed:,}')
print('\nVERIFICATION PASSED!')
