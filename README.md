# Coderhouse - Pre-entrega 3
Este proyecto es parte de una serie de entregas para el curso de Coder House. La presente versión corresponde a la pre-entrega 3, donde se construye a partir de la pre-entrega 2 donde se logró la consulta de la API elegida, la transformación de datos y carga a redshift, para en esta instancia implementar el ETL con airflow corriendo en Docker.

## COMENTARIO IMPORTANTE
Nuevamente no logré insertar los datos en la base de redshift. El error recibido es "'Connection' object has no attribute 'cursor'".
Intentando instalar las librerías con las que me comentaste que funciona, encontré errores. Hoy tenía pensado instalar una vm en azure desde cero para revisar el comportamiento, pero con la tormenta estoy sin luz.

En este caso quería intentar hacer un análisis más profundo sobre el tipo de objeto que trae en airflow, ya que noté algunas diferencias con el error anterior y puede que no se trate de un objeto "connection". Pero no llegué a hacer ese análisis. 

Si se requiere más info para entender el error, quedo atento para enviarla. 

### API BCRA
La API elegida es la del Banco central de la República Argentina. 
Link de la documentación de la API: https://estadisticasbcra.com/api/documentacion 

En este proyecto se consultan los siguientes endpoints: 
* "/plazo_fijo": monto en plazos fijos expresado en miles.
* "/depositos": monto en depósitos expresado en miles.
* "/cajas_ahorro": monto en cajas de ahorro expresado en miles.
* "/cuentas_corrientes": monto en cuentas corrientes expresado en miles.
* "/usd": cotización del dólar blue.
* "/usd_of": cotización del dólar oficial.

## Descripción del Proyecto
Hay algunos cambios respecto a la entrega anterior, ya que tuve que modificar los scripts y adaptar todo a funciones para hacer la migración a Airflow.

En esta ocasión, la estructura es la siguiente: 

`docker-compose.yaml`
    Contiene todas las configuraciones necesarias para iniciar los contenedores de docker. Si bien está disponible el archivo "requirements.txt", las librerías necesarias fueron agregadas en la primera línea de configuración del environment the x-airflow para que el contenedor instale las dependencias al crearse.

`requirements.txt`
    Contiene las librerías necesarias para que el ETL funcione correctamente.

Carpeta "dags"
    `dag_bcra-new.py`
        Este script contiene el DAG par la configuración de Airflow. Se agregan funciones para completar el proceso de requests a la API, con los distintos endpoints y se consolidan para componer un único df. También se instancia el DAG y se definen las tasks y el orden de ejecución. 

Carpeta "scripts"
    `api_fetch.py`
        Contiene el script para el pedido a la API
    `utils.py`
        Contiene funciones complementarias para conexiones.

`utils.py` Es el archivo de funciones que script principal utiliza para la lectura de credenciales de la API, construcción del _conn_string_ y conexión a Redshift + carga de datos en la base de datos.

`config.ini` Es el archivo de credenciales que consulta utils.py para autenticación y datos de conexión. A continuación se detalla la estructura. 

### Estructura del archivo `config.ini`
El archivo `config.ini` debe tener la siguiente estructura:

[api_bcra]
token = TU_TOKEN_AQUI

[redshift]
host = TU_HOST_AQUI
dbname = TU_DBNAME_AQUI
user = TU_USER_AQUI
password = TU_PASSWORD_AQUI
port = TU_PORT_AQUI

El archivo example-config.ini facilita la creación, únicamente solicitando la inserción de credenciales y datos propios. Es necesario también cambiar el nombre a "config.ini". 

### Ejecución
Para ejecutar correctamente el ETL es necesario posicionarse sobre la carpeta raíz y ejecutar el siguiente comando:

`docker-compose up -d`

Una vez ejecutado con éxito, se podrá acceder al webserver, desde donde se podrá observar el DAG creado que contiene las tareas que componen el ETL.
