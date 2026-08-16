ALTER TABLE dag_task ADD COLUMN version INT NOT NULL DEFAULT 1;
UPDATE dag_task SET version = COALESCE((SELECT version FROM dag WHERE dag.id = dag_task.dag_id), 1);
ALTER TABLE dag_task DROP CONSTRAINT uq_dag_task_name;
ALTER TABLE dag_task ADD CONSTRAINT uq_dag_task_name_version UNIQUE (dag_id, name, version);
