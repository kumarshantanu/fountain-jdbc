package org.bituf.fountain.jdbc;

import java.lang.reflect.UndeclaredThrowableException;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.transaction.support.CallbackPreferringPlatformTransactionManager;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;


@SuppressWarnings("serial")
public class CustomTransactionTemplate extends TransactionTemplate {
    
    protected final PlatformTransactionManager transactionManager;
    protected final TransactionAttribute transactionAttribute;
    
    public CustomTransactionTemplate(PlatformTransactionManager transactionManager) {
        super(transactionManager);
        Assert.notNull(transactionManager, "`transactionManager` must not be null");
        this.transactionManager = transactionManager;
        this.transactionAttribute = new DefaultTransactionAttribute();
    }
    
    public CustomTransactionTemplate(PlatformTransactionManager transactionManager,
            TransactionAttribute transactionAttribute) {
        super(transactionManager, transactionAttribute);
        Assert.notNull(transactionManager, "`transactionManager` must not be null");
        Assert.notNull(transactionAttribute, "`transactionAttribute` must not be null");
        this.transactionManager = transactionManager;
        this.transactionAttribute = transactionAttribute;
    }
    
    public TransactionAttribute getTransactionAttribute() {
        return transactionAttribute;
    }
    
    @Override
    public <T> T execute(TransactionCallback<T> action)
            throws TransactionException {
        if (this.transactionManager instanceof CallbackPreferringPlatformTransactionManager) {
            return ((CallbackPreferringPlatformTransactionManager) this.transactionManager).execute(this, action);
        }
        else {
            TransactionStatus status = this.transactionManager.getTransaction(this);
            T result;
            try {
                result = action.doInTransaction(status);
            }
            catch (RuntimeException ex) {
                if (this.transactionAttribute.rollbackOn(ex)) {
                    // Transactional code threw application exception -> rollback
                    rollbackOnException(status, ex);
                } else {
                    this.transactionManager.commit(status);
                }
                throw ex;
            }
            catch (Error err) {
                if (this.transactionAttribute.rollbackOn(err)) {
                    // Transactional code threw error -> rollback
                    rollbackOnException(status, err);
                } else {
                    this.transactionManager.commit(status);
                }
                throw err;
            }
            catch (Exception ex) {
                if (this.transactionAttribute.rollbackOn(ex)) {
                    // Transactional code threw unexpected exception -> rollback
                    rollbackOnException(status, ex);
                } else {
                    this.transactionManager.commit(status);
                }
                throw new UndeclaredThrowableException(ex, "TransactionCallback threw undeclared checked exception");
            }
            this.transactionManager.commit(status);
            return result;
        }
    }
    
    /**
     * Perform a rollback, handling rollback exceptions properly.
     * @param status object representing the transaction
     * @param ex the thrown application exception or error
     * @throws TransactionException in case of a rollback error
     */
    private void rollbackOnException(TransactionStatus status, Throwable ex) throws TransactionException {
        logger.debug("Initiating transaction rollback on application exception", ex);
        try {
            this.transactionManager.rollback(status);
        }
        catch (TransactionSystemException ex2) {
            logger.error("Application exception overridden by rollback exception", ex);
            ex2.initApplicationException(ex);
            throw ex2;
        }
        catch (RuntimeException ex2) {
            logger.error("Application exception overridden by rollback exception", ex);
            throw ex2;
        }
        catch (Error err) {
            logger.error("Application exception overridden by rollback error", ex);
            throw err;
        }
    }

}
