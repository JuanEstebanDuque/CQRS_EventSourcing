# Entregable 1: Prueba de endpoints

Juan Esteban Duque

Para hacer las pruebas levanté Kafka, MongoDB y MySQL con Docker (el archivo docker-compose.yml está en la raíz del repo) y corrí los dos microservicios: el de comandos en el puerto 5000 y el de consultas en el 5001.

Antes de poder correr el proyecto tuve que arreglar dos cosas. La primera es que no compilaba con Java 21 por la versión de Lombok que traía, así que la subí a la 1.18.34 en los pom.xml. La segunda la encontré probando: cuando se hacía un retiro, el saldo en la base de lectura no cambiaba. Revisando el código vi que en AccountEventHandler se calculaba el nuevo saldo pero nunca se guardaba en la cuenta, faltaba la línea `setBalance`. La agregué y ya los retiros se reflejan bien.

La prueba la hice en este orden: abrí una cuenta con 1000, deposité 500, retiré 200, hice las consultas, cerré la cuenta y al final restauré la base de lectura. Si todo funciona, el saldo antes de cerrar debería ser 1300.

## 1. Abrir cuenta (OpenAccountController)

POST `http://localhost:5000/api/v1/openBankAccount`

```json
{ "accountHolder": "Juan Duque", "accountType": "SAVINGS", "openingBalance": 1000 }
```

Respondió 201 y me devolvió el id de la cuenta nueva (cc9ac6a3-5cda-4880-97e2-9e1f975285a8). Ese id es el que usé en el resto de pruebas.

![Abrir cuenta](capturas/parte1/01-open-account.png)

## 2. Depositar (DepositFundsController)

PUT `http://localhost:5000/api/v1/depositFunds/{id}`

```json
{ "amount": 500 }
```

Respondió 200 con el mensaje de que el depósito se hizo.

![Depositar](capturas/parte1/02-deposit-funds.png)

## 3. Retirar (WithdrawFundsController)

PUT `http://localhost:5000/api/v1/withdrawFunds/{id}`

```json
{ "amount": 200 }
```

También respondió 200.

![Retirar](capturas/parte1/03-withdraw-funds.png)

## 4. Cerrar cuenta (CloseAccountController)

DELETE `http://localhost:5000/api/v1/closeBankAccount/{id}`

No lleva body. Respondió 200.

![Cerrar cuenta](capturas/parte1/04-close-account.png)

Para confirmar que sí se cerró, volví a buscar la cuenta por id y ya no aparece (204, sin contenido), porque al cerrarse se borra de la base de lectura.

![Cuenta cerrada](capturas/parte1/04b-get-by-id-after-close.png)

## 5. Consultas (AccountLookupController)

Estas las hice antes de cerrar la cuenta, para que hubiera algo que mostrar.

a. Todas las cuentas: GET `http://localhost:5001/api/v1/bankAccountLookup/`

![Todas las cuentas](capturas/parte1/05a-get-all-accounts.png)

b. Por id: GET `http://localhost:5001/api/v1/bankAccountLookup/byId/{id}`. Aquí se ve el saldo en 1300, que es lo que esperaba.

![Por id](capturas/parte1/05b-get-by-id.png)

c. Por titular: GET `http://localhost:5001/api/v1/bankAccountLookup/byHolder/Juan Duque`

![Por titular](capturas/parte1/05c-get-by-holder.png)

d. Por balance: GET `http://localhost:5001/api/v1/bankAccountLookup/withBalance/GREATER_THAN/1000`. Busca las cuentas con saldo mayor a 1000 (también se puede usar LESS_THAN).

![Por balance](capturas/parte1/05d-get-with-balance.png)

## 6. Restaurar la base de lectura (RestoreReadDbController)

POST `http://localhost:5000/api/v1/restoreReadDb`

No lleva body. Respondió 201. Lo que hace es tomar los eventos guardados en MongoDB y volver a mandarlos a Kafka para reconstruir la base de lectura.

![Restaurar](capturas/parte1/06-restore-read-db.png)

Algo que noté es que si se llama este endpoint cuando la base de lectura ya tiene datos, los depósitos y retiros se vuelven a aplicar encima y el saldo queda mal. Entonces sirve más que todo cuando la base de lectura está vacía o se perdió.
