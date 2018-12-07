/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Modifier;

import org.junit.Assume;
import org.junit.rules.MethodRule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.voltdb.FlakyTestRule.FlakyTestIgnore;

public class FlakyTestRule implements MethodRule {

    /**
     * The @Flaky annotation is intended to be used for JUnit tests that do not
     * pass reliably, failing either intermittently or consistently; such tests
     * may or may not be skipped, depending on several factors:
     *     o The value of the system property FOO, when running the tests
     *     o
     * but may be run by specifying a system property. Once they pass
     * reliably again, the @Flaky annotation may be removed.
     * <p>
     * Optionally, you may specify a string giving a description of the test's
     * flakiness. ???
     * TODO
     */
    @Documented
    @Target(ElementType.METHOD)
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Flaky {
        boolean isFlaky() default true;
        String description() default "";
        Class<? extends FlakyTestIgnore> condition() default FlakyTestIgnoreImpl.class;
    }

    public interface FlakyTestIgnore {
        boolean ignoreFlakyTest();
    }

//    @Override  // needed for MethodRule ???
//    public Statement apply(Statement arg0, FrameworkMethod arg1, Object arg2) {
//        // TODO Auto-generated method stub
//        return null;
//    }

//    @Override  // needed for TestRule ??
//    public Statement apply(Statement arg0, Description arg1) {
//        System.out.println("In FTR.apply; base, method, target: "+base+", "+method+", "+target);
//        // TODO Auto-generated method stub
//        return null;
//    }

    @Override
    public Statement apply(Statement base, FrameworkMethod method, Object target) {
        //System.out.println("\nDEBUG: In FTR.apply; base, method, target: "+base+", "+method+", "+target);
        Statement result = base;
        if (hasFlakyAnnotation(method) ) {
            FlakyTestIgnore condition = getFlakyTestIgnore( target, method );
            if (condition.ignoreFlakyTest()) {
                result = new FlakyTestStatement( condition );
            }
        }
        return result;
    }

    private static boolean hasFlakyAnnotation( FrameworkMethod method ) {
        //System.out.println("DEBUG: In FTR.hasFlakyAnnotation; method: "+method);
        return method.getAnnotation( Flaky.class ) != null;
    }

    private static FlakyTestIgnore getFlakyTestIgnore( Object target, FrameworkMethod method ) {
        //System.out.println("DEBUG: In FTR.getFlakyTestIgnore; target, method: "+target+", "+method);
        Flaky annotation = method.getAnnotation( Flaky.class );
        return new FlakyTestIgnoreCreator( target, annotation ).create();
    }

    private static class FlakyTestIgnoreCreator {
        private final Object target;
        private final Class<? extends FlakyTestIgnore> conditionType;

        FlakyTestIgnoreCreator( Object target, Flaky annotation ) {
            this.target = target;
            this.conditionType = annotation.condition();
            //System.out.println("DEBUG: In FTR.IFTC constructor; target, annotation, conditionType: "
            //                   +target+", "+annotation+", "+conditionType);
        }

        FlakyTestIgnore create() {
            //System.out.println("DEBUG: In FTR.IFTC.create");
            checkConditionType();
            try {
                return createCondition();
            } catch( RuntimeException re ) {
                throw re;
            } catch( Exception e ) {
                throw new RuntimeException( e );
            }
        }

        private FlakyTestIgnore createCondition() throws Exception {
            //System.out.println("DEBUG: In FTR.IFTC.createCondition");
            FlakyTestIgnore result;
            if( isConditionTypeStandalone() ) {
                result = conditionType.newInstance();
            } else {
                result = conditionType.getDeclaredConstructor( target.getClass() ).newInstance( target );
            }
            //System.out.println("DEBUG:     result: "+result);
            return result;
        }

      private void checkConditionType() {
          //System.out.println("DEBUG: In FTR.IFTC.checkConditionType");
          if( !isConditionTypeStandalone() && !isConditionTypeDeclaredInTarget() ) {
              String msg
                = "Conditional class '%s' is a member class "
                + "but was not declared inside the test case using it.\n"
                + "Either make this class a static class, "
                + "standalone class (by declaring it in it's own file) "
                + "or move it inside the test case using it";
              throw new IllegalArgumentException( String.format ( msg, conditionType.getName() ) );
          }
      }

      private boolean isConditionTypeStandalone() {
          //System.out.println("DEBUG: In FTR.IFTC.isConditionTypeStandalone");
          return !conditionType.isMemberClass() || Modifier.isStatic( conditionType.getModifiers() );
      }

      private boolean isConditionTypeDeclaredInTarget() {
          //System.out.println("DEBUG: In FTR.IFTC.isConditionTypeDeclaredInTarget");
          return target.getClass().isAssignableFrom( conditionType.getDeclaringClass() );
      }
    }



    private static class FlakyTestStatement extends Statement {
        private final FlakyTestIgnore condition;

        FlakyTestStatement( FlakyTestIgnore condition ) {
            //System.out.println("DEBUG: In FTR.FTS constructor; condition: "+condition);
            this.condition = condition;
        }

        @Override
        public void evaluate() {
            System.out.println("DEBUG: In FTR.FTS.evaluate: @Flaky test will be skipped");
            Assume.assumeTrue(false);
        }
    }

}
