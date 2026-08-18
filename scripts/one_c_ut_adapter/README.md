# Проверочный клиент адаптера 1С:УТ 11

Клиент нужен до подключения тестовой базы 1С. Он проверяет авторизацию,
версию контракта, получение пакета, повторяемость ключей и обратное
подтверждение.

По умолчанию выполняется только безопасный `ping` без получения документов:

```bash
export DZ_API_BASE_URL="https://dragonzap.online"
export ONE_C_EXCHANGE_LOGIN="onec"
export ONE_C_EXCHANGE_PASSWORD="..."
python scripts/one_c_ut_adapter/client.py
```

Получить и показать пакет без подтверждения:

```bash
python scripts/one_c_ut_adapter/client.py --mode pull
```

Имитировать создание документов и подтвердить пакет:

```bash
python scripts/one_c_ut_adapter/client.py \
  --mode process \
  --state-file var/one_c_adapter_state.json
```

`process` нельзя запускать против рабочей базы как замену настоящей 1С:
он присваивает тестовые идентификаторы `SIM-*` и помечает события успешно
переданными. Для проверки ошибочного ответа можно передать
`--fail-event-uid <UUID>`.
