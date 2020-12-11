import path from 'path';
import {
  EntityManager,
  Entity,
  MikroORM,
  PrimaryKey,
  Property,
  EventSubscriber,
} from '@mikro-orm/core';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';
import {
  DockerComposeEnvironment,
  StartedDockerComposeEnvironment,
} from 'testcontainers';
import Docker, { Container } from 'dockerode';
import { v4 as uuid } from 'uuid';

@Entity({ tableName: 'users' })
class User {
  @PrimaryKey()
  id!: number;

  @Property()
  readonly username: string;

  constructor(username: string) {
    this.username = username;
  }
}

async function getOrmInstance(
  subscriber?: EventSubscriber
): Promise<MikroORM<PostgreSqlDriver>> {
  const orm = await MikroORM.init({
    type: 'postgresql',
    user: 'postgres',
    password: 'somepassword',
    dbName: 'test_db',
    port: 15432,
    entities: [User],
    subscribers: subscriber ? [subscriber] : [],
  });

  return orm as MikroORM<PostgreSqlDriver>;
}

describe('the tests', () => {
  let environment: StartedDockerComposeEnvironment;
  let dbContainer: Container;
  let orm: MikroORM;
  let em: EntityManager;
  const testSubscriber: EventSubscriber = {
    afterCreate: async () => {},
    afterFlush: async () => {},
  };
  const afterCreateSpy = jest.spyOn(testSubscriber, 'afterCreate');
  const afterFlushSpy = jest.spyOn(testSubscriber, 'afterFlush');

  async function getLogs(lines: number) {
    const buffer = ((await dbContainer.logs({
      follow: false,
      stdout: true,
      stderr: true,
      tail: lines,
    })) as unknown) as Buffer;
    const logs = buffer.toString();
    return logs;
  }

  beforeAll(async () => {
    environment = await new DockerComposeEnvironment(
      path.resolve(__dirname, '..'),
      'docker-compose.yaml'
    ).up();
    const docker = new Docker();
    dbContainer = docker.getContainer('mikro-orm-tx-tests-db');
    orm = await getOrmInstance(testSubscriber);
  }, 600_000);

  afterAll(async () => {
    await orm.close();
    await environment.down();
  });

  beforeEach(async () => {
    em = orm.em.fork();
  });

  afterEach(async () => {
    afterCreateSpy.mockClear();
    afterFlushSpy.mockClear();
  });

  describe('immediate constraint (failures on insert)', () => {
    beforeAll(async () => {
      const orm = await getOrmInstance();
      await orm.em.getConnection().execute(
        `
          drop table if exists users;
          create table users (
            id serial primary key,
            username varchar(50) not null unique deferrable initially immediate
          );
          `
      );
      await orm.close();
    });
    describe('implicit transactions', () => {
      let username: string;
      it('hooks called when inserting a new user succeeds', async () => {
        username = uuid();
        const user = new User(username);
        em.persist(user);
        await em.flush();

        expect(afterCreateSpy).toHaveBeenCalledTimes(1);
        expect(afterFlushSpy).toHaveBeenCalledTimes(1);
      });
      it('afterCreate hook not called when inserting a new user fails', async () => {
        const user = new User(username);
        em.persist(user);
        await expect(() => em.flush()).rejects.toThrowError(
          /^insert.+duplicate key value/
        );

        expect(afterCreateSpy).toHaveBeenCalledTimes(0);
      });
      it('afterFlush hook not called when inserting a new user fails', async () => {
        const user = new User(username);
        em.persist(user);
        await expect(() => em.flush()).rejects.toThrowError(
          /^insert.+duplicate key value/
        );

        expect(afterFlushSpy).toHaveBeenCalledTimes(0);
      });
      it('automatic rollback sent on failure', async () => {
        const user = new User(username);
        em.persist(user);
        await expect(() => em.flush()).rejects.toThrowError(
          /^insert.+duplicate key value/
        );
        const logs = await getLogs(1);
        expect(logs).toContain('ROLLBACK');
      });
    });

    describe('explicit transactions with "transactional()"', () => {
      let username: string;
      it('hooks called when inserting a new user succeeds', async () => {
        await em.transactional(async (em) => {
          username = uuid();
          const user = new User(username);
          em.persist(user);
        });

        expect(afterCreateSpy).toHaveBeenCalledTimes(1);
        expect(afterFlushSpy).toHaveBeenCalledTimes(1);
      });
      it('afterCreate hook not called when inserting a new user fails', async () => {
        const work = async () => {
          await em.transactional(async (em) => {
            const user = new User(username);
            em.persist(user);
          });
        };

        await expect(work).rejects.toThrowError(/^insert.+duplicate key value/);
        expect(afterCreateSpy).toHaveBeenCalledTimes(0);
      });
      it('afterFlush hook not called when inserting a new user fails', async () => {
        const work = async () => {
          await em.transactional(async (em) => {
            const user = new User(username);
            em.persist(user);
          });
        };

        await expect(work).rejects.toThrowError(/^insert.+duplicate key value/);
        expect(afterFlushSpy).toHaveBeenCalledTimes(0);
      });
      it('automatic rollback sent on failure', async () => {
        const work = async () => {
          await em.transactional(async (em) => {
            const user = new User(username);
            em.persist(user);
          });
        };

        await expect(work).rejects.toThrowError(/^insert.+duplicate key value/);
        const logs = await getLogs(1);
        expect(logs).toContain('ROLLBACK');
      });
    });

    describe('explicit transactions with explicit begin/commit method calls', () => {
      let username: string;
      it('hooks called when inserting a new user succeeds', async () => {
        await em.begin();
        username = uuid();
        const user = new User(username);
        em.persist(user);
        await em.commit();

        expect(afterCreateSpy).toHaveBeenCalledTimes(1);
        expect(afterFlushSpy).toHaveBeenCalledTimes(1);
      });
      it('afterCreate hook not called when inserting a new user fails', async () => {
        await em.begin();
        const work = async () => {
          try {
            const user = new User(username);
            em.persist(user);
            await em.commit();
          } catch (error) {
            await em.rollback();
            throw error;
          }
        };

        await expect(work).rejects.toThrowError(/^insert.+duplicate key value/);
        expect(afterCreateSpy).toHaveBeenCalledTimes(0);
      });
      it('afterFlush hook not called when inserting a new user fails', async () => {
        await em.begin();
        const work = async () => {
          try {
            const user = new User(username);
            em.persist(user);
            await em.commit();
          } catch (error) {
            await em.rollback();
            throw error;
          }
        };

        await expect(work).rejects.toThrowError(/^insert.+duplicate key value/);
        expect(afterFlushSpy).toHaveBeenCalledTimes(0);
      });
      it('automatic rollback sent on failure', async () => {
        await em.begin();
        const work = async () => {
          try {
            const user = new User(username);
            em.persist(user);
            await em.commit();
          } catch (error) {
            await em.rollback();
            throw error;
          }
        };

        await expect(work).rejects.toThrowError(/^insert.+duplicate key value/);
        const logs = await getLogs(1);
        expect(logs).toContain('ROLLBACK');
      });
    });
  });

  describe('deferred constraints (failures on commit)', () => {
    beforeAll(async () => {
      const orm = await getOrmInstance();
      await orm.em.getConnection().execute(
        `
        drop table if exists users;
        create table users (
          id serial primary key,
          username varchar(50) not null unique deferrable initially deferred
        );
          `
      );
      await orm.close();
    });

    describe('implicit transactions', () => {
      let username: string;
      it('hooks called when inserting a new user succeeds', async () => {
        username = uuid();
        const user = new User(username);
        em.persist(user);
        await em.flush();

        expect(afterCreateSpy).toHaveBeenCalledTimes(1);
        expect(afterFlushSpy).toHaveBeenCalledTimes(1);
      });
      it('afterCreate hook not called when inserting a new user fails', async () => {
        const user = new User(username);
        em.persist(user);
        await expect(em.flush()).rejects.toThrowError(
          /^COMMIT.+duplicate key value/
        );

        expect(afterCreateSpy).toHaveBeenCalledTimes(0);
      });
      it('afterFlush hook not called when inserting a new user fails', async () => {
        const user = new User(username);
        em.persist(user);
        await expect(em.flush()).rejects.toThrowError(
          /^COMMIT.+duplicate key value/
        );

        expect(afterFlushSpy).toHaveBeenCalledTimes(0);
      });
      it('automatic rollback sent on failure', async () => {
        const user = new User(username);
        em.persist(user);

        await expect(em.flush()).rejects.toThrowError(
          /^COMMIT.+duplicate key value/
        );
        const logs = await getLogs(1);
        expect(logs).toContain('ROLLBACK');
      });
    });

    describe('explicit transactions with "transactional()"', () => {
      let username: string;
      it('hooks called when inserting a new user succeeds', async () => {
        await em.transactional(async (em) => {
          username = uuid();
          const user = new User(username);
          em.persist(user);
        });

        expect(afterCreateSpy).toHaveBeenCalledTimes(1);
        expect(afterFlushSpy).toHaveBeenCalledTimes(1);
      });
      it('afterCreate hook not called when inserting a new user fails', async () => {
        const work = async () => {
          await em.transactional(async (em) => {
            const user = new User(username);
            em.persist(user);
          });
        };

        await expect(work).rejects.toThrowError(/^COMMIT.+duplicate key value/);
        expect(afterCreateSpy).toHaveBeenCalledTimes(0);
      });
      it('afterFlush hook not called when inserting a new user fails', async () => {
        const work = async () => {
          await em.transactional(async (em) => {
            const user = new User(username);
            em.persist(user);
          });
        };

        await expect(work).rejects.toThrowError(/^COMMIT.+duplicate key value/);
        expect(afterFlushSpy).toHaveBeenCalledTimes(0);
      });
      it('automatic rollback sent on failure', async () => {
        const work = async () => {
          await em.transactional(async (em) => {
            const user = new User(username);
            em.persist(user);
          });
        };

        await expect(work).rejects.toThrowError(/^COMMIT.+duplicate key value/);
        const logs = await getLogs(1);
        expect(logs).toContain('ROLLBACK');
      });
    });

    describe('explicit transactions with explicit begin/commit method calls', () => {
      let username: string;
      it('hooks called when inserting a new user succeeds', async () => {
        await em.begin();
        username = uuid();
        const user = new User(username);
        em.persist(user);
        await em.commit();

        expect(afterCreateSpy).toHaveBeenCalledTimes(1);
        expect(afterFlushSpy).toHaveBeenCalledTimes(1);
      });
      it('commit should throw when inserting a new user fails', async () => {
        await em.begin();
        const work = async () => {
          try {
            const user = new User(username);
            em.persist(user);
            await em.commit();
          } catch (error) {
            await em.rollback();
            throw error;
          }
        };

        await expect(work).rejects.toThrowError(/^COMMIT.+duplicate key value/);
      });
      it('afterCreate hook not called when inserting a new user fails', async () => {
        await em.begin();
        const work = async () => {
          try {
            const user = new User(username);
            em.persist(user);
            await em.commit();
          } catch (error) {
            await em.rollback();
            throw error;
          }
        };

        await expect(work).rejects.toThrowError(/^COMMIT.+duplicate key value/);
        expect(afterCreateSpy).toHaveBeenCalledTimes(0);
      });
      it('afterFlush not called when inserting a new user fails', async () => {
        await em.begin();
        const work = async () => {
          try {
            const user = new User(username);
            em.persist(user);
            await em.commit();
          } catch (error) {
            await em.rollback();
            throw error;
          }
        };

        await expect(work).rejects.toThrowError(/^COMMIT.+duplicate key value/);
        expect(afterFlushSpy).toHaveBeenCalledTimes(0);
      });
      it('automatic rollback sent on failure', async () => {
        await em.begin();
        const work = async () => {
          try {
            const user = new User(username);
            em.persist(user);
            await em.commit();
          } catch (error) {
            await em.rollback();
            throw error;
          }
        };

        await expect(work).rejects.toThrowError(/^COMMIT.+duplicate key value/);
        const logs = await getLogs(1);
        expect(logs).toContain('ROLLBACK');
      });
    });
  });
});
