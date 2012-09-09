package org.honeysoft.monogo.testbed;

import com.mongodb.DB;
import com.mongodb.Mongo;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;

/**
 * Created with IntelliJ IDEA.
 * User: ihristov
 * Date: 9/1/12
 * Time: 11:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class MongoManager implements TestRule {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private Coordinator coordinator;

    @Override
    public Statement apply(final Statement base, Description description) {

        coordinator = Coordinator.getOrCreate();

        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                base.evaluate();
                coordinator.stopDb();
            }
        };
    }

}
