package com.buildathon.orchestrator.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

@Entity
@Table(name = "task_dependency")
public class TaskDependencyEntity {

    @EmbeddedId
    private TaskDependencyId id;

    protected TaskDependencyEntity() {
    }

    public TaskDependencyEntity(UUID taskId, UUID dependsOnTaskId) {
        this.id = new TaskDependencyId(taskId, dependsOnTaskId);
    }

    public UUID getTaskId() {
        return id.taskId;
    }

    public UUID getDependsOnTaskId() {
        return id.dependsOnTaskId;
    }

    @Embeddable
    public static class TaskDependencyId implements Serializable {

        @Column(name = "task_id", nullable = false)
        private UUID taskId;

        @Column(name = "depends_on_task_id", nullable = false)
        private UUID dependsOnTaskId;

        protected TaskDependencyId() {
        }

        public TaskDependencyId(UUID taskId, UUID dependsOnTaskId) {
            this.taskId = taskId;
            this.dependsOnTaskId = dependsOnTaskId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TaskDependencyId that)) {
                return false;
            }
            return Objects.equals(taskId, that.taskId) && Objects.equals(dependsOnTaskId, that.dependsOnTaskId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskId, dependsOnTaskId);
        }
    }
}
