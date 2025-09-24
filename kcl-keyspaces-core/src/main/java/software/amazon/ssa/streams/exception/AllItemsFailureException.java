package software.amazon.ssa.streams.exception;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Exception thrown when all items in a batch operation fail.
 * This exception contains details about all the failed items and their corresponding error messages.
 */
public class AllItemsFailureException extends Exception {
    
    private static final long serialVersionUID = 1L;
    
    private final String connectorName;
    private final List<String> errorMessages;
    private final int totalItems;
    
    /**
     * Constructs a new AllItemsFailureException with a list of error messages.
     * 
     * @param connectorName Name of the connector that failed
     * @param errorMessages List of error messages describing the failures
     * @param totalItems Total number of items in the batch
     */
    public AllItemsFailureException(String connectorName, List<String> errorMessages, int totalItems) {
        super(createMessage(connectorName, errorMessages, totalItems));
        this.connectorName = connectorName;
        this.errorMessages = errorMessages != null ? new ArrayList<>(errorMessages) : new ArrayList<>();
        this.totalItems = totalItems;
    }
    
    /**
     * Constructs a new AllItemsFailureException with a list of error messages and a cause.
     * 
     * @param connectorName Name of the connector that failed
     * @param errorMessages List of error messages describing the failures
     * @param totalItems Total number of items in the batch
     * @param cause The cause of the exception
     */
    public AllItemsFailureException(String connectorName, List<String> errorMessages, int totalItems, Throwable cause) {
        super(createMessage(connectorName, errorMessages, totalItems), cause);
        this.connectorName = connectorName;
        this.errorMessages = errorMessages != null ? new ArrayList<>(errorMessages) : new ArrayList<>();
        this.totalItems = totalItems;
    }
    
    /**
     * Gets the name of the connector that failed.
     * 
     * @return Connector name
     */
    public String getConnectorName() {
        return connectorName;
    }
    
    /**
     * Gets the list of error messages.
     * 
     * @return Unmodifiable list of error messages
     */
    public List<String> getErrorMessages() {
        return Collections.unmodifiableList(errorMessages);
    }
    
    /**
     * Gets the total number of items in the batch.
     * 
     * @return Total number of items
     */
    public int getTotalItems() {
        return totalItems;
    }
    
    /**
     * Gets the number of items that failed (always equals totalItems for this exception).
     * 
     * @return Number of failed items
     */
    public int getFailedItems() {
        return totalItems;
    }
    
    /**
     * Gets the number of items that succeeded (always 0 for this exception).
     * 
     * @return Number of successful items (always 0)
     */
    public int getSuccessfulItems() {
        return 0;
    }
    
    /**
     * Gets the success rate as a percentage (always 0.0 for this exception).
     * 
     * @return Success rate (always 0.0)
     */
    public double getSuccessRate() {
        return 0.0;
    }
    
    /**
     * Checks if all items failed (always true for this exception).
     * 
     * @return Always true
     */
    public boolean isCompleteFailure() {
        return true;
    }
    
    /**
     * Checks if some items succeeded (always false for this exception).
     * 
     * @return Always false
     */
    public boolean hasPartialSuccess() {
        return false;
    }
    
    /**
     * Creates a formatted error message.
     * 
     * @param connectorName Name of the connector
     * @param errorMessages List of error messages
     * @param totalItems Total number of items
     * @return Formatted error message
     */
    private static String createMessage(String connectorName, List<String> errorMessages, int totalItems) {
        StringBuilder message = new StringBuilder();
        message.append("Complete failure in ").append(connectorName).append(": all ").append(totalItems)
               .append(" items failed");
        
        if (errorMessages != null && !errorMessages.isEmpty()) {
            message.append(". Error details: ");
            if (errorMessages.size() <= 5) {
                // Show all errors if 5 or fewer
                message.append(String.join("; ", errorMessages));
            } else {
                // Show first 5 errors and indicate there are more
                message.append(String.join("; ", errorMessages.subList(0, 5)))
                       .append("; and ").append(errorMessages.size() - 5).append(" more errors");
            }
        }
        
        return message.toString();
    }
    
    @Override
    public String toString() {
        return getClass().getSimpleName() + ": " + getMessage();
    }
}
