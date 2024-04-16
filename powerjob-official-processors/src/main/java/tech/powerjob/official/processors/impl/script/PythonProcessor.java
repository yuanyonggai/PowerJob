package tech.powerjob.official.processors.impl.script;

import org.springframework.stereotype.Component;

/**
 * python processor
 *
 * @author tjq
 * @since 2021/2/7
 */
@Component("PythonProcessor")
public class PythonProcessor extends AbstractScriptProcessor {

    @Override
    protected String getScriptName(Long instanceId) {
        return String.format("python_%d.py", instanceId);
    }

    @Override
    protected String getRunCommand() {
        return "python";
    }
}
