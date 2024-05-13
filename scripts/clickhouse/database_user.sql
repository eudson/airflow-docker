-- Comandos para garantir que começas do zero
DROP DATABASE IF EXISTS scda_analysis ON CLUSTER cluster1;
DROP USER IF EXISTS deploy;

-- Criar a base de dados para analises
CREATE DATABASE scda_analysis ON CLUSTER cluster1 COMMENT 'Base de para analises dos dados de auditoria';

-- Criar o user que ira correr as queries
CREATE USER deploy ON CLUSTER cluster1 IDENTIFIED WITH sha256_password BY 'deploy';

-- Dar permissoes ao user
GRANT ALL PRIVILEGES ON scda_analysis.* TO deploy ON CLUSTER cluster1 WITH GRANT OPTION;
-- grant on cluster testcluster all on *.* to testuser with grant option
-- GRANT ON CLUSTER cluster1 ALL ON *.* TO deploy  WITH GRANT OPTION;
-- Verificar que a base de dados foi criada com sucesso
SELECT name, comment FROM system.databases WHERE name = 'scda_analysis';

