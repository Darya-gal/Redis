Практика часть 2.
Лабораторная работа по Redis
Выполнила: Дарья Галынина 24КНТ-2
Задания:4.c — все различные районы сотрудников
        5.c — номер, дата, часы для записей с платой > 100000 руб.
        6.b — название работы, должность, дата, плата
        7.d — номер работы, организация, фамилия (Советский район) + сортировка
        10.c — должности, где Пивоваров работал более одного раза
        11.d — организации, где Алексанов работал более одного раза
        12 — объединение адресов сотрудников и организаций
        13.d — сотрудник и должность, работавшие во всех организациях Нижегородского или Сормовского районов
        14.d — работники с платой выше средней
        15.d — среднее количество часов по организациям Нижегородского или Сормовского районов
# 
Выбрала пункт d,так как в некоторых номерах нет пункта d,делала последнюю букву в номере.
Работа выполнена в терминале VS Code.Подключение Redis через Extentions+Docker
# Запуск контейнера с Redis
docker run -d --name redis -p 6379:6379 redis

# Проверка, что Redis работает
docker ps | grep redis
Код для загрузки данных:
docker exec -i redis redis-cli << 'EOF'
FLUSHALL

HSET employee:1 name "Пивоваров" district "Канавинский" tax 10
HSET employee:2 name "Махалина" district "Сормовский" tax 10
HSET employee:3 name "Щанников" district "Нижегородский" tax 15
HSET employee:4 name "Воробьев" district "Советский" tax 15
HSET employee:5 name "Алексанов" district "Советский" tax 10

HSET workplace:1 name "Университет" district "Приокский" pension 20
HSET workplace:2 name "Институт механики" district "Приокский" pension 10
HSET workplace:3 name "Технический университет" district "Нижегородский" pension 20
HSET workplace:4 name "НИИ ПМК" district "Нижегородский" pension 10
HSET workplace:5 name "Сельхоз академия" district "Приокский" pension 20
HSET workplace:6 name "Академия бизнеса" district "Сормовский" pension 25

HSET position:1 name "Ассистент" rate 10000 max_hours 40
HSET position:2 name "Старший преподаватель" rate 15000 max_hours 35
HSET position:3 name "Доцент" rate 20000 max_hours 20
HSET position:4 name "Профессор" rate 25000 max_hours 10
HSET position:5 name "Мл. научный сотрудник" rate 7000 max_hours 60
HSET position:6 name "Ст. научный сотрудник" rate 10000 max_hours 50
HSET position:7 name "Зав. лабораторией" rate 13000 max_hours 40

HSET work:20000 employee 3 month "Январь" workplace 4 position 7 hours 10 pay 130000
HSET work:20001 employee 5 month "Январь" workplace 6 position 4 hours 5 pay 125000
HSET work:20002 employee 1 month "Февраль" workplace 6 position 1 hours 35 pay 350000
HSET work:20003 employee 2 month "Февраль" workplace 2 position 5 hours 10 pay 70000
HSET work:20004 employee 2 month "Февраль" workplace 1 position 2 hours 30 pay 450000
HSET work:20005 employee 2 month "Февраль" workplace 5 position 1 hours 10 pay 100000
HSET work:20006 employee 3 month "Февраль" workplace 2 position 3 hours 15 pay 300000
HSET work:20007 employee 1 month "Апрель" workplace 1 position 2 hours 20 pay 300000
HSET work:20008 employee 2 month "Апрель" workplace 5 position 6 hours 40 pay 400000
HSET work:20009 employee 4 month "Апрель" workplace 5 position 1 hours 10 pay 100000
HSET work:20010 employee 2 month "Май" workplace 2 position 2 hours 20 pay 300000
HSET work:20011 employee 3 month "Июнь" workplace 6 position 3 hours 11 pay 220000
HSET work:20012 employee 1 month "Июль" workplace 3 position 2 hours 10 pay 150000
HSET work:20013 employee 2 month "Июль" workplace 2 position 3 hours 15 pay 300000
HSET work:20014 employee 4 month "Август" workplace 2 position 4 hours 8 pay 200000
HSET work:20015 employee 5 month "Август" workplace 2 position 7 hours 10 pay 130000
HSET work:20016 employee 1 month "Август" workplace 3 position 2 hours 20 pay 300000
EOF
Запросы:
# 4.c
docker exec -it redis redis-cli --raw EVAL "
local districts={}
for _,k in ipairs(redis.call('KEYS','employee:*')) do
  districts[redis.call('HGET',k,'district')]=true
end
local res={}
for d,_ in pairs(districts) do
  table.insert(res,d)
end
table.sort(res)
return res
" 0

# 5.c
docker exec -it redis redis-cli --raw EVAL "
local res={}
for _,k in ipairs(redis.call('KEYS','work:*')) do
  if tonumber(redis.call('HGET',k,'pay'))>100000 then
    local id=string.match(k,'%d+$')
    table.insert(res,{'Номер: '..id,'Дата: '..redis.call('HGET',k,'month'),'Часы: '..redis.call('HGET',k,'hours')})
  end
end
return res
" 0

# 6.b
docker exec -it redis redis-cli --raw EVAL "
local res={}
for _,k in ipairs(redis.call('KEYS','work:*')) do
  local wp_id=redis.call('HGET',k,'workplace')
  local pos_id=redis.call('HGET',k,'position')
  table.insert(res,{
    'Орг: '..redis.call('HGET','workplace:'..wp_id,'name'),
    'Долж: '..redis.call('HGET','position:'..pos_id,'name'),
    'Дата: '..redis.call('HGET',k,'month'),
    'Плата: '..redis.call('HGET',k,'pay')
  })
end
return res
" 0

# 7.d
docker exec -it redis redis-cli --raw EVAL "
local res={}
for _,k in ipairs(redis.call('KEYS','work:*')) do
  local emp_id=redis.call('HGET',k,'employee')
  local district=redis.call('HGET','employee:'..emp_id,'district')
  if district=='Советский' then
    local wp_id=redis.call('HGET',k,'workplace')
    table.insert(res,{
      work=string.match(k,'%d+$'),
      org=redis.call('HGET','workplace:'..wp_id,'name'),
      emp=redis.call('HGET','employee:'..emp_id,'name')
    })
  end
end
table.sort(res,function(a,b) return a.org<b.org end)
local fmt={}
for _,v in ipairs(res) do
  table.insert(fmt,'Работа #'..v.work..' | '..v.org..' | '..v.emp)
end
return fmt
" 0

# 10.c
docker exec -it redis redis-cli --raw EVAL "
local pos_count={}
for _,k in ipairs(redis.call('KEYS','work:*')) do
  local emp_id=redis.call('HGET',k,'employee')
  local name=redis.call('HGET','employee:'..emp_id,'name')
  if name=='Пивоваров' then
    local pos_id=redis.call('HGET',k,'position')
    pos_count[pos_id]=(pos_count[pos_id] or 0)+1
  end
end
local res={}
for pos_id,cnt in pairs(pos_count) do
  if cnt>1 then
    table.insert(res,redis.call('HGET','position:'..pos_id,'name'))
  end
end
return res
" 0

# 11.d

docker exec -it redis redis-cli --raw EVAL "
local wp_count={}
for _,k in ipairs(redis.call('KEYS','work:*')) do
  local emp_id=redis.call('HGET',k,'employee')
  local name=redis.call('HGET','employee:'..emp_id,'name')
  if name=='Алексанов' then
    local wp_id=redis.call('HGET',k,'workplace')
    wp_count[wp_id]=(wp_count[wp_id] or 0)+1
  end
end
local res={}
for wp_id,cnt in pairs(wp_count) do
  if cnt>1 then
    local wp_name=redis.call('HGET','workplace:'..wp_id,'name')
    local pension=redis.call('HGET','workplace:'..wp_id,'pension')
    table.insert(res,wp_name..' | отчисления: '..pension..'%')
  end
end
if #res==0 then
  return {'Алексанов не работал более одного раза в одной организации'}
end
return res
" 0

# 12

docker exec -it redis redis-cli --raw EVAL "
local addrs={}
for _,k in ipairs(redis.call('KEYS','employee:*')) do
  table.insert(addrs,'Сотрудник: '..redis.call('HGET',k,'district'))
end
for _,k in ipairs(redis.call('KEYS','workplace:*')) do
  table.insert(addrs,'Организация: '..redis.call('HGET',k,'district'))
end
table.sort(addrs)
return addrs
" 0

# 13.d

docker exec -it redis redis-cli --raw EVAL "
local target={}
for _,k in ipairs(redis.call('KEYS','workplace:*')) do
  local d=redis.call('HGET',k,'district')
  if d=='Нижегородский' or d=='Сормовский' then
    target[string.match(k,'%d+$')]=true
  end
end
local total=0
for _ in pairs(target) do
  total=total+1
end
if total==0 then
  return {'Нет организаций'}
end
local stats={}
for _,k in ipairs(redis.call('KEYS','work:*')) do
  local wp_id=redis.call('HGET',k,'workplace')
  if target[wp_id] then
    local emp_id=redis.call('HGET',k,'employee')
    local pos_id=redis.call('HGET',k,'position')
    local key=emp_id..':'..pos_id
    if not stats[key] then
      stats[key]={}
    end
    stats[key][wp_id]=true
  end
end
local res={}
for key,orgs in pairs(stats) do
  local cnt=0
  for _ in pairs(orgs) do
    cnt=cnt+1
  end
  if cnt==total then
    local emp_id,pos_id=key:match('(%d+):(%d+)')
    local emp_name=redis.call('HGET','employee:'..emp_id,'name')
    local pos_name=redis.call('HGET','position:'..pos_id,'name')
    table.insert(res,emp_name..' | '..pos_name)
  end
end
return res
" 0

# 14.d

docker exec -it redis redis-cli --raw EVAL "
local sum,cnt=0,0
for _,k in ipairs(redis.call('KEYS','work:*')) do
  sum=sum+tonumber(redis.call('HGET',k,'pay'))
  cnt=cnt+1
end
local avg=sum/cnt
local emps={}
for _,k in ipairs(redis.call('KEYS','work:*')) do
  if tonumber(redis.call('HGET',k,'pay'))>avg then
    local emp_id=redis.call('HGET',k,'employee')
    emps[redis.call('HGET','employee:'..emp_id,'name')]=true
  end
end
local res={'Средняя плата: '..string.format('%.2f',avg)}
for name,_ in pairs(emps) do
  table.insert(res,name)
end
return res
" 0


# 15.d

docker exec -it redis redis-cli --raw EVAL "
local orgs={}
for _,k in ipairs(redis.call('KEYS','workplace:*')) do
  local d=redis.call('HGET',k,'district')
  if d=='Нижегородский' or d=='Сормовский' then
    local id=string.match(k,'%d+$')
    orgs[id]={name=redis.call('HGET',k,'name'),sum=0,cnt=0}
  end
end
for _,k in ipairs(redis.call('KEYS','work:*')) do
  local wp_id=redis.call('HGET',k,'workplace')
  if orgs[wp_id] then
    orgs[wp_id].sum=orgs[wp_id].sum+tonumber(redis.call('HGET',k,'hours'))
    orgs[wp_id].cnt=orgs[wp_id].cnt+1
  end
end
local res={}
for _,data in pairs(orgs) do
  local avg=data.cnt>0 and data.sum/data.cnt or 0
  table.insert(res,data.name..': '..string.format('%.2f',avg)..' ч')
end
table.sort(res)
return res
" 0


