package demo.csms.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Registers a bean only when this process runs the given role ({@code csms.roles}, from
 * {@code CSMS_ROLES}): the same artifact is deployed with all roles for small setups, or as
 * separate deployments scaled independently.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@Conditional(ConditionalOnRole.OnRole.class)
public @interface ConditionalOnRole {

    String WORKER = "worker";
    String API = "api";
    String DASHBOARD = "dashboard";

    /** The role that must be enabled. */
    String value();

    /** A role that must not be enabled, if any. */
    String unless() default "";

    class OnRole implements Condition {

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            Map<String, Object> attributes = metadata.getAnnotationAttributes(ConditionalOnRole.class.getName());
            String roles = context.getEnvironment().getProperty("csms.roles", "worker,api,dashboard");
            Set<String> enabled = Arrays.stream(roles.split(",")).map(String::trim).collect(Collectors.toSet());
            String unless = (String) attributes.get("unless");
            return enabled.contains((String) attributes.get("value")) && (unless.isEmpty() || !enabled.contains(unless));
        }
    }
}
