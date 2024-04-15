package tech.powerjob.common.utils;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import javax.script.ScriptException;
import java.util.List;

public class SqlFormatterUtils {

    /**
     * Default prefix for single-line comments within SQL scripts: {@code "--"}.
     */
    public static final String DEFAULT_COMMENT_PREFIX = "--";
    /**
     * Default start delimiter for block comments within SQL scripts: {@code "/*"}.
     */
    public static final String DEFAULT_BLOCK_COMMENT_START_DELIMITER = "/*";
    /**
     * Default end delimiter for block comments within SQL scripts: <code>"*&#47;"</code>.
     */
    public static final String DEFAULT_BLOCK_COMMENT_END_DELIMITER = "*/";

    public static String format(String source) {
        //return SqlFormatter.extend(cfg -> cfg.plusStringTypes(Arrays.asList("\"\"", "''", "``", "[]"))).format(source);
        return source;
    }

    public static void splitSqlScript(String script, String separator, List<String> statements) throws ScriptException {
        splitSqlScript(script, separator, DEFAULT_COMMENT_PREFIX, DEFAULT_BLOCK_COMMENT_START_DELIMITER, DEFAULT_BLOCK_COMMENT_END_DELIMITER, statements);
    }

    public static void splitSqlScript(String script, String separator, String commentPrefix, String blockCommentStartDelimiter, String blockCommentEndDelimiter, List<String> statements) throws ScriptException {
        Assert.hasText(commentPrefix, "'commentPrefix' must not be null or empty");
        splitSqlScript(script, separator, new String[]{commentPrefix}, blockCommentStartDelimiter, blockCommentEndDelimiter, statements);
    }

    public static void splitSqlScript(String script, String separator, String[] commentPrefixes, String blockCommentStartDelimiter, String blockCommentEndDelimiter, List<String> statements) throws ScriptException {
        Assert.hasText(script, "'script' must not be null or empty");
        Assert.notNull(separator, "'separator' must not be null");
        Assert.notEmpty(commentPrefixes, "'commentPrefixes' must not be null or empty");
        for (String commentPrefix : commentPrefixes) {
            Assert.hasText(commentPrefix, "'commentPrefixes' must not contain null or empty elements");
        }
        Assert.hasText(blockCommentStartDelimiter, "'blockCommentStartDelimiter' must not be null or empty");
        Assert.hasText(blockCommentEndDelimiter, "'blockCommentEndDelimiter' must not be null or empty");
        StringBuilder sb = new StringBuilder();
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inEscape = false;
        for (int i = 0; i < script.length(); i++) {
            char c = script.charAt(i);
            if (inEscape) {
                inEscape = false;
                sb.append(c);
                continue;
            }
            // MySQL style escapes
            if (c == '\\') {
                inEscape = true;
                sb.append(c);
                continue;
            }
            if (!inDoubleQuote && (c == '\'')) {
                inSingleQuote = !inSingleQuote;
            } else if (!inSingleQuote && (c == '"')) {
                inDoubleQuote = !inDoubleQuote;
            }
            if (!inSingleQuote && !inDoubleQuote) {
                if (script.startsWith(separator, i)) {
                    // We've reached the end of the current statement
                    if (sb.length() > 0) {
                        statements.add(sb.toString());
                        sb = new StringBuilder();
                    }
                    i += separator.length() - 1;
                    continue;
                } else if (startsWithAny(script, commentPrefixes, i)) {
                    // Skip over any content from the start of the comment to the EOL
                    int indexOfNextNewline = script.indexOf('\n', i);
                    if (indexOfNextNewline > i) {
                        i = indexOfNextNewline;
                        continue;
                    } else {
                        // If there's no EOL, we must be at the end of the script, so stop here.
                        break;
                    }
                } else if (script.startsWith(blockCommentStartDelimiter, i)) {
                    // Skip over any block comments
                    int indexOfCommentEnd = script.indexOf(blockCommentEndDelimiter, i);
                    if (indexOfCommentEnd > i) {
                        i = indexOfCommentEnd + blockCommentEndDelimiter.length() - 1;
                        continue;
                    } else {
                        throw new RuntimeException("Missing block comment end delimiter: " + blockCommentEndDelimiter);
                    }
                } else if (c == ' ' || c == '\r' || c == '\n' || c == '\t') {
                    // Avoid multiple adjacent whitespace characters
                    if (sb.length() > 0 && sb.charAt(sb.length() - 1) != ' ') {
                        c = ' ';
                    } else {
                        continue;
                    }
                }
            }
            sb.append(c);
        }
        if (StringUtils.hasText(sb)) {
            statements.add(sb.toString());
        }
    }

    private static boolean startsWithAny(String script, String[] prefixes, int offset) {
        for (String prefix : prefixes) {
            if (script.startsWith(prefix, offset)) {
                return true;
            }
        }
        return false;
    }
}