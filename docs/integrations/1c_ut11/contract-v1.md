# Контракт DZ-1C Outbox 1.0

## Авторизация

Все методы адаптера используют HTTP Basic Auth. Учётные данные задаются на
сервере переменными `ONE_C_EXCHANGE_LOGIN` и `ONE_C_EXCHANGE_PASSWORD`.

## Проверка подключения

`GET /1c/outbox/v1/ping`

Метод не читает и не изменяет очередь.

## Получение пакета

`GET /1c/outbox/v1/query?limit=100`

```json
{
  "protocol": "dz-1c-outbox",
  "contract_version": "1.0",
  "batch_uid": "2ac53d35-19d8-4bd5-a728-36dd14795039",
  "content_hash": "sha256...",
  "formed_at": "2026-08-11T12:00:00+03:00",
  "attempt": 1,
  "events": [
    {
      "event_uid": "6c702694-22f1-4d14-a32c-372eef557e47",
      "entity_type": "shipment",
      "entity_id": 123,
      "event_type": "posted",
      "payload_version": 1,
      "idempotency_key": "shipment:123:posted:...",
      "payload": {}
    }
  ]
}
```

Если очередь пуста, `batch_uid` и `content_hash` равны `null`, а `events` пуст.
Пока пакет не подтверждён, повторный запрос возвращает тот же `batch_uid` и
содержимое. Поле `attempt` увеличивается.

Денежные значения в `payload` передаются строками с десятичной точкой, даты и
время в ISO 8601, количества целыми числами.

## Подтверждение пакета

`POST /1c/outbox/v1/{batch_uid}/ack`

```json
{
  "contract_version": "1.0",
  "results": [
    {
      "event_uid": "6c702694-22f1-4d14-a32c-372eef557e47",
      "success": true,
      "external_id": "GUID-документа-1С"
    },
    {
      "event_uid": "84bbde9a-7654-4bb8-888f-4d99ffbfbe40",
      "success": false,
      "error": "Не найден договор контрагента"
    }
  ]
}
```

В детальном режиме результат обязателен для каждого события пакета. Неизвестный
или пропущенный `event_uid` отклоняет подтверждение с HTTP 409. Для обратной
совместимости прежний клиент может отправить общий `success`, `error` и словарь
`external_ids` без массива `results`.

## Версионирование

Версия транспортного контракта и `payload_version` решают разные задачи:

- `contract_version` описывает конверт пакета и подтверждение;
- `payload_version` описывает снимок конкретного бизнес-документа.

Несовместимое изменение публикуется новым URL `/outbox/v2/...`; существующий
`v1` продолжает работать до завершения миграции 1С.

