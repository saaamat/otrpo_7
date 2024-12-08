## 1. Установить и запустить RabbitMQ
## 2. Скопировать файлы и перейти в директорию проекта
```cmd
cd ./otrpo_7
```
## 3. Устанавливаем зависимости
```cmd
pip install -r req.txt
```
## 4. Указываем свои значения в .env
```
RABBITMQ_HOST=''
RABBITMQ_PORT=
RABBITMQ_USER=''
RABBITMQ_PASSWORD=''
```
## 5. Запуск Producer-a
```python
#Пример
python producer.py https://www.utmn.ru/
```
## 6. Запуск Consumer-а
```python
python consumer.py
```
### Producer находит все внутренние ссылки (только для этого домена) в HTML коде (a[href]), помещает их в очередь RabbitMQ по одной. 
### Consumer читает из очереди эти ссылки, также находит внутренние ссылки и помещает в их очередь,

