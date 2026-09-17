# Atividade Prática 01: Enquadramento NDJSON e Handshake de Registro

## Contexto e Objetivo
Esta é a atividade inicial do projeto P2P com Balanceamento de Carga Dinâmico e serve de preparação direta para a Sprint 1. 

**Objetivo:** Estabelecer um canal TCP confiável entre dois processos distintos, utilizando um protocolo de mensagens em JSON por linha (NDJSON), com identidade persistente. É o alicerce para identificar e corrigir erros antes da Sprint principal.

## Roteiro

1. **Separação de Processos:** 
   Partindo do material "Exemplo Socket", separe o código em dois executáveis: `master.py` e `worker.py`. Cada um deve ter seu próprio arquivo de configuração (`config.json`) contendo:
   * UUID (gerado na primeira execução e persistido)
   * `label`
   * `host`
   * `port`

2. **Enquadramento NDJSON:** 
   Implemente as funções `send_msg` e `recv_msgs`. 
   * Cada mensagem deve ser um JSON serializado em uma única linha, terminada por quebra de linha. 
   * O receptor deve manter um buffer para acumular bytes parciais e entregar apenas linhas completas.

3. **Envelope de Mensagem:** 
   Defina e valide o envelope mínimo. Campos obrigatórios: `type`, `msg_id`, `request_id`, `origin` (UUID e label), `timestamp` e `payload`.
   * **Validação:** Mensagens sem campos obrigatórios ou com JSON inválido devem ser descartadas e registradas no log. Isso **nunca** deve derrubar o processo.

4. **Handshake de Registro:** 
   Implemente as mensagens `register_worker` e `registration_ack`. 
   * O master deve manter uma tabela de workers em memória, indexada por UUID. 
   * **Reconexão:** A reconexão de um mesmo UUID deve atualizar o registro existente, não criar uma duplicata.

5. **Registro de Logs:** 
   Registre logs em uma única linha por evento, seguindo estritamente o formato: 
   `grupo | origem | destino | type | request_id | resultado`

## Testes a Executar
Cada grupo deve demonstrar os cinco cenários abaixo em execução, com os logs à vista:

* **Fragmentação:** Enviar um JSON grande em dois envios (`send`) e confirmar que chega inteiro.
* **Coalescência:** Enviar três mensagens em rajada e confirmar que o receptor entrega três, não uma.
* **JSON Inválido:** Enviar conteúdo malformado e confirmar que o processo sobrevive.
* **Identidade Duplicada:** Subir dois workers com o mesmo UUID e descrever o comportamento escolhido.
* **Reconexão:** Encerrar um worker, subir novamente e mostrar que a tabela do master permanece com um único registro.

## Resultado Esperado
Um master e pelo menos um worker executando como processos independentes, identificáveis pelo `label` nos logs, trocando mensagens enquadradas corretamente (mesmo sob fragmentação e rajada), com registro reconhecido pelo master e reconexão sem duplicar o cadastro.