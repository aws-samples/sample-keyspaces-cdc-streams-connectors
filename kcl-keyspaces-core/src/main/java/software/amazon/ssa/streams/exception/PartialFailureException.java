package software.amazon.ssa.streams.exception;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Exception thrown when a batch operation partially fails.
 * This exception contains details about which specific items in the batch failed
 * and their corresponding error messages.
 */
public class PartialFailureException extends Exception {
    
    private static final long serialVersionUID = 1L;
    
    private final List<String> errorMessages;
    private final int totalItems;
    private final int failedItems;
    
    /**
     * Constructs a new PartialFailureException with a list of error messages.
     * 
     * @param errorMessages List of error messages describing the failures
     * @param totalItems Total number of items in the batch
     * @param failedItems Number of items that failed
     */
    public PartialFailureException(String connectorName, List<String> errorMessages, int totalItems, int failedItems) {
        super(createMessage(errorMessages, totalItems, failedItems));
        this.errorMessages = errorMessages != null ? new ArrayList<>(errorMessages) : new ArrayList<>();
        this.totalItems = totalItems;
        this.failedItems = failedItems;
    }
    
    /**
     * Constructs a new PartialFailureException with a list of error messages and a cause.
     * 
     * @param errorMessages List of error messages describing the failures
     * @param totalItems Total number of items in the batch
     * @param failedItems Number of items that failed
     * @param cause The cause of the exception
     */
    public PartialFailureException(String connectorName, List<String> errorMessages, int totalItems, int failedItems, Throwable cause) {
        super(createMessage(errorMessages, totalItems, failedItems), cause);
        this.errorMessages = errorMessages != null ? new ArrayList<>(errorMessages) : new ArrayList<>();
        this.totalItems = totalItems;
        this.failedItems = failedItems;
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
     * Gets the number of items that failed.
     * 
     * @return Number of failed items
     */
    public int getFailedItems() {
        return failedItems;
    }
    
    /**
     * Gets the number of items that succeeded.
     * 
     * @return Number of successful items
     */
    public int getSuccessfulItems() {
        return totalItems - failedItems;
    }
    
    /**
     * Gets the success rate as a percentage.
     * 
     * @return Success rate (0.0 to 100.0)
     */
    public double getSuccessRate() {
        if (totalItems == 0) {
            return 100.0;
        }
        return ((double) getSuccessfulItems() / totalItems) * 100.0;
    }
    
    /**
     * Checks if all items failed.
     * 
     * @return true if all items failed, false otherwise
     */
    public boolean isCompleteFailure() {
        return failedItems == totalItems;
    }
    
    /**
     * Checks if some items succeeded.
     * 
     * @return true if at least one item succeeded, false otherwise
     */
    public boolean hasPartialSuccess() {
        return failedItems < totalItems;
    }
    
    /**
     * Creates a formatted error message.
     * 
     * @param errorMessages List of error messages
     * @param totalItems Total number of items
     * @param failedItems Number of failed items
     * @return Formatted error message
     */
    private static String createMessage(List<String> errorMessages, int totalItems, int failedItems) {
        StringBuilder message = new StringBuilder();
        message.append("Partial failure: ").append(failedItems).append(" of ").append(totalItems)
               .append(" items failed");
        
        if (errorMessages != null && !errorMessages.isEmpty()) {
            message.append(". Error details: ");
            if (errorMessages.size() <= 3) {
                // Show all errors if 3 or fewer
                message.append(String.join("; ", errorMessages));
            } else {
                // Show first 3 errors and indicate there are more
                message.append(String.join("; ", errorMessages.subList(0, 3)))
                       .append("; and ").append(errorMessages.size() - 3).append(" more errors");
            }
        }
        
        return message.toString();
    }
    
    @Override
    public String toString() {
        return getClass().getSimpleName() + ": " + getMessage();
    }
}
