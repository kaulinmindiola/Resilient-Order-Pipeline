package io.github.kaulinmindiola.rop.inventory;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/** Hexagonal dependency rules (ADR-0006, REQ-MAINT-001). */
@AnalyzeClasses(
        packages = "io.github.kaulinmindiola.rop.inventory",
        importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchRule domainIsFreeOfInfrastructure =
            noClasses()
                    .that()
                    .resideInAPackage("..domain..")
                    .should()
                    .dependOnClassesThat()
                    .resideInAnyPackage(
                            "org.springframework..",
                            "jakarta.persistence..",
                            "org.apache.kafka..",
                            "tools.jackson..",
                            "com.fasterxml.jackson..");

    @ArchTest
    static final ArchRule domainDependsOnNothingAbove =
            noClasses()
                    .that()
                    .resideInAPackage("..domain..")
                    .should()
                    .dependOnClassesThat()
                    .resideInAnyPackage("..application..", "..adapter..");

    @ArchTest
    static final ArchRule applicationDoesNotDependOnAdapters =
            noClasses()
                    .that()
                    .resideInAPackage("..application..")
                    .should()
                    .dependOnClassesThat()
                    .resideInAPackage("..adapter..");
}
